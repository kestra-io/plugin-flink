package io.kestra.plugin.flink;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import io.kestra.core.utils.RetryUtils;
import io.kestra.core.models.tasks.retrys.Exponential;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Cancel a running Flink job.",
    description = "This task cancels a running Flink job. Optionally, it can trigger a savepoint " +
                  "before cancellation to preserve the job state."
)
@Plugin(
    examples = {
        @Example(
            title = "Cancel a job with savepoint",
            full = true,
            code = """
                id: cancel-flink-job
                namespace: company.team

                tasks:
                  - id: cancel-job
                    type: io.kestra.plugin.flink.Cancel
                    restUrl: "http://flink-jobmanager:8081"
                    jobId: "{{ inputs.jobId }}"
                    withSavepoint: true
                    savepointDir: "s3://flink/savepoints/canceled/{{ execution.id }}"
                    drainJob: true
                """
        ),
        @Example(
            title = "Force cancel without savepoint",
            code = """
                id: force-cancel
                type: io.kestra.plugin.flink.Cancel
                restUrl: "http://flink-jobmanager:8081"
                jobId: "{{ inputs.jobId }}"
                withSavepoint: false
                """
        )
    }
)
public class Cancel extends Task implements RunnableTask<Cancel.Output> {

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();

    @Schema(
        title = "Flink REST API URL",
        description = "The base URL of the Flink cluster's REST API, e.g., 'http://flink-jobmanager:8081'"
    )
    @NotNull
    private Property<String> restUrl;

    @Schema(
        title = "Job ID",
        description = "The ID of the Flink job to cancel"
    )
    @NotNull
    private Property<String> jobId;

    @Schema(
        title = "Create savepoint before cancellation",
        description = "Whether to trigger a savepoint before canceling the job. Defaults to false."
    )
    @Builder.Default
    private Property<Boolean> withSavepoint = Property.of(false);

    @Schema(
        title = "Savepoint directory",
        description = "Target directory for the savepoint. Required if withSavepoint is true."
    )
    private Property<String> savepointDir;

    @Schema(
        title = "Drain job",
        description = "Whether to drain the job (process all remaining input) before cancellation. " +
                      "Only applicable for streaming jobs. Defaults to false."
    )
    @Builder.Default
    private Property<Boolean> drainJob = Property.of(false);

    @Schema(
        title = "Cancellation timeout",
        description = "Maximum time to wait for cancellation to complete in seconds. Defaults to 60."
    )
    @Builder.Default
    private Property<Integer> cancellationTimeout = Property.of(60);

    @Override
    public Cancel.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rRestUrl = runContext.render(this.restUrl).as(String.class).orElseThrow();
        String rJobId = runContext.render(this.jobId).as(String.class).orElseThrow();
        Boolean withSp = runContext.render(this.withSavepoint).as(Boolean.class).orElse(false);
        Boolean drain = runContext.render(this.drainJob).as(Boolean.class).orElse(false);

        logger.info("Cancelling Flink job: {} (withSavepoint: {}, drain: {})", rJobId, withSp, drain);

        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();

        String savepointPath = null;

        if (withSp) {
            // Trigger savepoint before cancellation
            savepointPath = triggerSavepoint(runContext, client, rRestUrl, rJobId);
            logger.info("Savepoint created at: {}", savepointPath);
        }

        // Cancel the job
        String cancellationResult = cancelJob(runContext, client, rRestUrl, rJobId, drain);

        // Wait for job to be canceled
        waitForJobCancellation(runContext, client, rRestUrl, rJobId);

        logger.info("Successfully cancelled job: {}", rJobId);

        return Output.builder()
            .jobId(rJobId)
            .savepointPath(savepointPath)
            .cancellationResult(cancellationResult)
            .build();
    }

    private String triggerSavepoint(RunContext runContext, HttpClient client, String restUrl, String jobId)
            throws Exception {

        String savepointDirectory = null;
        if (savepointDir != null) {
            savepointDirectory = runContext.render(savepointDir).as(String.class).orElse(null);
        }

        if (savepointDirectory == null) {
            throw new IllegalArgumentException("savepointDir is required when withSavepoint is true");
        }

        String payload = "{\"targetDirectory\":\"" + savepointDirectory + "\",\"cancelJob\":false}";

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(restUrl + "/v1/jobs/" + jobId + "/savepoints"))
            .timeout(Duration.ofMinutes(10))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 202) {
            throw new RuntimeException("Failed to trigger savepoint: " + response.statusCode() + " - " + response.body());
        }

        // Extract request ID and wait for completion
        String requestId = extractRequestIdFromResponse(response.body());
        return waitForSavepointCompletion(runContext, client, restUrl, jobId, requestId);
    }

    private String cancelJob(RunContext runContext, HttpClient client, String restUrl, String jobId, boolean drain)
            throws IOException, InterruptedException {

        String endpoint = drain ? "/v1/jobs/" + jobId + "/stop" : "/v1/jobs/" + jobId;
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(restUrl + endpoint))
            .timeout(Duration.ofSeconds(60));

        HttpRequest request;
        if (drain) {
            // For drain, we need to POST with stop request
            request = requestBuilder
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"drain\":true}"))
                .build();
        } else {
            // For regular cancel, we DELETE
            request = requestBuilder
                .DELETE()
                .build();
        }

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 202) {
            throw new RuntimeException("Failed to cancel job: " + response.statusCode() + " - " + response.body());
        }

        return drain ? "Job stop requested (drain mode)" : "Job cancellation requested";
    }

    private void waitForJobCancellation(RunContext runContext, HttpClient client, String restUrl, String jobId)
            throws Exception {

        int timeoutSeconds = runContext.render(cancellationTimeout).as(Integer.class).orElse(60);
        int maxAttempts = timeoutSeconds / 2; // Check every 2 seconds

        new RetryUtils().<Boolean, Exception>of(
            Exponential.builder()
                .delayFactor(1.0) // Fixed interval
                .interval(Duration.ofSeconds(2))
                .maxInterval(Duration.ofSeconds(2))
                .maxAttempts(maxAttempts)
                .build()
        ).run(
            (result, throwable) -> {
                // Don't retry if job is canceled/finished (result = true)
                if (result != null && result) {
                    return false;
                }
                // Don't retry on fatal exceptions
                if (throwable instanceof RuntimeException) {
                    String message = throwable.getMessage();
                    if (message != null && message.contains("Job cancellation timed out")) {
                        return false;
                    }
                }
                // Continue retrying
                return true;
            },
            () -> {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(restUrl + "/v1/jobs/" + jobId))
                    .timeout(Duration.ofSeconds(30))
                    .GET()
                    .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 404) {
                    // Job not found, likely canceled
                    return true;
                }

                if (response.statusCode() == 200) {
                    String state = extractJobStateFromResponse(response.body());
                    if ("CANCELED".equals(state) || "FINISHED".equals(state)) {
                        return true;
                    }
                }

                // Return null to continue polling
                return null;
            }
        );
    }

    private String waitForSavepointCompletion(RunContext runContext, HttpClient client, String restUrl,
                                            String jobId, String requestId) throws Exception {

        int timeoutSeconds = 300; // 5 minutes for savepoint
        int maxAttempts = timeoutSeconds / 5; // Check every 5 seconds

        return new RetryUtils().<String, Exception>of(
            Exponential.builder()
                .delayFactor(1.0) // Fixed interval
                .interval(Duration.ofSeconds(5))
                .maxInterval(Duration.ofSeconds(5))
                .maxAttempts(maxAttempts)
                .build()
        ).run(
            (result, throwable) -> {
                // Don't retry if we got a successful result
                if (result != null) {
                    return false;
                }
                // Don't retry on fatal exceptions
                if (throwable instanceof RuntimeException) {
                    String message = throwable.getMessage();
                    if (message != null && (message.contains("Savepoint failed:") || message.contains("Failed to check savepoint status:"))) {
                        return false;
                    }
                }
                // Retry on other cases
                return true;
            },
            () -> {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(restUrl + "/v1/jobs/" + jobId + "/savepoints/" + requestId))
                    .timeout(Duration.ofSeconds(30))
                    .GET()
                    .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    throw new RuntimeException("Failed to check savepoint status: " + response.statusCode() + " - " + response.body());
                }

                String status = extractSavepointStatusFromResponse(response.body());
                if ("COMPLETED".equals(status)) {
                    return extractSavepointPathFromResponse(response.body());
                } else if ("FAILED".equals(status)) {
                    String error = extractSavepointErrorFromResponse(response.body());
                    throw new RuntimeException("Savepoint failed: " + error);
                }

                // Return null to continue polling
                return null;
            }
        );
    }

    private String extractRequestIdFromResponse(String responseBody) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(responseBody);
            String requestId = root.path("request-id").asText(null);
            if (requestId == null) {
                throw new RuntimeException("Could not extract request ID from response: " + responseBody);
            }
            return requestId;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not parse savepoint trigger response", e);
        }
    }

    private String extractJobStateFromResponse(String responseBody) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(responseBody);
            return root.path("state").asText("UNKNOWN");
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not parse job status response", e);
        }
    }

    private String extractSavepointStatusFromResponse(String responseBody) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(responseBody);
            return root.path("status").path("id").asText("UNKNOWN");
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not parse savepoint status response", e);
        }
    }

    private String extractSavepointPathFromResponse(String responseBody) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(responseBody);
            JsonNode location = root.path("operation").path("location");
            return location.isMissingNode() || location.isNull() ? null : location.asText();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not parse savepoint status response", e);
        }
    }

    private String extractSavepointErrorFromResponse(String responseBody) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(responseBody);
            JsonNode failure = root.path("operation").path("failure-cause");
            if (failure.isMissingNode() || failure.isNull()) {
                return "Unknown error";
            }
            JsonNode message = failure.path("message");
            if (message.isTextual()) {
                return message.asText();
            }
            return failure.toString();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not parse savepoint failure response", e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The cancelled job ID",
            description = "The ID of the Flink job that was cancelled"
        )
        private final String jobId;

        @Schema(
            title = "Savepoint path",
            description = "Path to the savepoint created before cancellation (if withSavepoint was true)"
        )
        private final String savepointPath;

        @Schema(
            title = "Cancellation result",
            description = "Result message from the cancellation operation"
        )
        private final String cancellationResult;

    }
}