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
import java.net.URI;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

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
                    type: io.kestra.plugin.flink.CancelJob
                    restUrl: "http://flink-jobmanager:8081"
                    jobId: "{{ inputs.jobId }}"
                    withSavepoint: true
                    savepointDir: "s3://flink/savepoints/canceled/{{ execution.id }}"
                    drainJob: true
                """
        ),
        @Example(
            title = "Force cancel without savepoint",
            full = true,
            code = """
                id: force-cancel-job
                namespace: company.team

                tasks:
                  - id: force-cancel
                    type: io.kestra.plugin.flink.CancelJob
                    restUrl: "http://flink-jobmanager:8081"
                    jobId: "{{ inputs.jobId }}"
                    withSavepoint: false
                """
        )
    }
)
public class CancelJob extends Task implements RunnableTask<CancelJob.Output> {

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
    public CancelJob.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rRestUrl = runContext.render(this.restUrl).as(String.class).orElseThrow();
        String rJobId = runContext.render(this.jobId).as(String.class).orElseThrow();
        Boolean withSp = runContext.render(this.withSavepoint).as(Boolean.class).orElse(false);
        Boolean drain = runContext.render(this.drainJob).as(Boolean.class).orElse(false);

        logger.info("Cancelling Flink job: {} (withSavepoint: {}, drain: {})", rJobId, withSp, drain);

        HttpClient client = HttpClient.builder()
            .runContext(runContext)
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

        Map<String, Object> payloadMap = new HashMap<>();
        payloadMap.put("targetDirectory", savepointDirectory);
        payloadMap.put("cancelJob", false);
        String payload = OBJECT_MAPPER.writeValueAsString(payloadMap);

        HttpRequest request = HttpRequest.builder()
            .uri(URI.create(restUrl + "/v1/jobs/" + jobId + "/savepoints"))
            .method("POST")
            .addHeader("Content-Type", "application/json")
            .body(HttpRequest.StringRequestBody.builder()
                .content(payload)
                .build())
            .build();

        HttpResponse<String> response = client.request(request, String.class);

        if (response.getStatus().getCode() != 202) {
            throw new RuntimeException("Failed to trigger savepoint: " + response.getStatus().getCode() + " - " + response.getBody());
        }

        // Extract request ID and wait for completion
        String requestId = extractRequestIdFromResponse(response.getBody());
        return waitForSavepointCompletion(runContext, client, restUrl, jobId, requestId);
    }

    private String cancelJob(RunContext runContext, HttpClient client, String restUrl, String jobId, boolean drain)
            throws Exception {

        String endpoint = drain ? "/v1/jobs/" + jobId + "/stop" : "/v1/jobs/" + jobId;

        HttpRequest request;
        if (drain) {
            // For drain, we need to POST with stop request
            request = HttpRequest.builder()
                .uri(URI.create(restUrl + endpoint))
                .method("POST")
                .addHeader("Content-Type", "application/json")
                .body(HttpRequest.StringRequestBody.builder()
                    .content("{\"drain\":true}")
                    .build())
                .build();
        } else {
            // For regular cancel, we DELETE
            request = HttpRequest.builder()
                .uri(URI.create(restUrl + endpoint))
                .method("DELETE")
                .build();
        }

        HttpResponse<String> response = client.request(request, String.class);

        if (response.getStatus().getCode() != 202) {
            throw new RuntimeException("Failed to cancel job: " + response.getStatus().getCode() + " - " + response.getBody());
        }

        return drain ? "Job stop requested (drain mode)" : "Job cancellation requested";
    }

    private void waitForJobCancellation(RunContext runContext, HttpClient client, String restUrl, String jobId)
            throws Exception {

        int timeoutSeconds = Math.max(1, runContext.render(cancellationTimeout).as(Integer.class).orElse(60));
        final int intervalSec = 2;
        final int maxAttempts = Math.max(1, (int) Math.ceil((double) timeoutSeconds / intervalSec));
        final java.time.Instant deadline = java.time.Instant.now().plusSeconds(timeoutSeconds);

        new RetryUtils().<Boolean, Exception>of(
            Exponential.builder()
                .delayFactor(1.0) // Fixed interval
                .interval(Duration.ofSeconds(intervalSec))
                .maxInterval(Duration.ofSeconds(intervalSec))
                .maxAttempts(maxAttempts)
                .build()
        ).run(
            (result, throwable) -> {
                if (result != null && result) return false;
                if (throwable instanceof NonRetriableCancellationException ||
                    throwable instanceof java.util.concurrent.TimeoutException) {
                    return false;
                }
                return true; // retry other exceptions
            },
            () -> {
                // Hard-stop if global timeout elapsed
                if (java.time.Instant.now().isAfter(deadline)) {
                    throw new java.util.concurrent.TimeoutException("Job cancellation timed out after " + timeoutSeconds + " seconds");
                }

                HttpRequest request = HttpRequest.builder()
                    .uri(URI.create(restUrl + "/v1/jobs/" + jobId))
                    .method("GET")
                    .build();

                HttpResponse<String> response = client.request(request, String.class);

                if (response.getStatus().getCode() == 404) {
                    // Job not found, likely canceled
                    return true;
                }

                int statusCode = response.getStatus().getCode();
                if (statusCode == 200) {
                    String state = extractJobStateFromResponse(response.getBody());
                    if ("FINISHED".equals(state) || "FAILED".equals(state) || "CANCELED".equals(state)) {
                        return true;
                    }
                } else if (statusCode >= 500 || statusCode == 429) {
                    runContext.logger().warn("Transient status {} from Flink; will retry. Body: {}", statusCode, response.getBody());
                    return null; // keep polling
                } else {
                    throw new NonRetriableCancellationException("Failed to check job status: " + statusCode + " - " + response.getBody());
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
                HttpRequest request = HttpRequest.builder()
                    .uri(URI.create(restUrl + "/v1/jobs/" + jobId + "/savepoints/" + requestId))
                    .method("GET")
                    .build();

                HttpResponse<String> response = client.request(request, String.class);

                if (response.getStatus().getCode() != 200) {
                    throw new RuntimeException("Failed to check savepoint status: " + response.getStatus().getCode() + " - " + response.getBody());
                }

                String status = extractSavepointStatusFromResponse(response.getBody());
                if ("COMPLETED".equals(status)) {
                    return extractSavepointPathFromResponse(response.getBody());
                } else if ("FAILED".equals(status)) {
                    String error = extractSavepointErrorFromResponse(response.getBody());
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

    private static final class NonRetriableCancellationException extends RuntimeException {
        NonRetriableCancellationException(String message) { super(message); }
    }

    private static final class NonRetriableSavepointException extends RuntimeException {
        NonRetriableSavepointException(String message) { super(message); }
    }
}