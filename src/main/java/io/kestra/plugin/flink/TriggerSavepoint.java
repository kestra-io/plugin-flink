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
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a savepoint for a running Flink job",
    description = "Requests a savepoint via the Flink REST API and waits for completion. Supports optional target directory, format type, and post-savepoint cancellation."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger savepoint with specific directory",
            full = true,
            code = """
                id: create-savepoint
                namespace: company.team

                tasks:
                  - id: trigger-savepoint
                    type: io.kestra.plugin.flink.TriggerSavepoint
                    restUrl: "http://flink-jobmanager:8081"
                    jobId: "{{ inputs.jobId }}"
                    targetDirectory: "s3://flink/savepoints/backup/{{ execution.id }}"
                    savepointTimeout: 300
                """
        ),
        @Example(
            title = "Trigger savepoint with default directory",
            code = """
                id: default-savepoint
                type: io.kestra.plugin.flink.TriggerSavepoint
                restUrl: "http://flink-jobmanager:8081"
                jobId: "{{ inputs.jobId }}"
                """
        )
    }
)
public class TriggerSavepoint extends Task implements RunnableTask<TriggerSavepoint.Output> {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Schema(
        title = "Flink REST API URL",
        description = "Base URL of the Flink REST API (e.g., `http://flink-jobmanager:8081`)."
    )
    @NotNull
    private Property<String> restUrl;

    @Schema(
        title = "Job ID",
        description = "ID of the Flink job to snapshot."
    )
    @NotNull
    private Property<String> jobId;

    @Schema(
        title = "Target directory",
        description = "Target directory for the savepoint; falls back to the cluster default when omitted."
    )
    private Property<String> targetDirectory;

    @Schema(
        title = "Cancel job after savepoint",
        description = "Cancel the job after the savepoint completes; defaults to false."
    )
    @Builder.Default
    private Property<Boolean> cancelJob = Property.of(false);

    @Schema(
        title = "Savepoint timeout",
        description = "Maximum wait time for savepoint completion in seconds; defaults to 300."
    )
    @Builder.Default
    private Property<Integer> savepointTimeout = Property.of(300);

    @Schema(
        title = "Format type",
        description = "Savepoint format type: CANONICAL or NATIVE. Defaults to CANONICAL for broader compatibility."
    )
    @Builder.Default
    private Property<String> formatType = Property.of("CANONICAL");

    @Override
    public TriggerSavepoint.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rRestUrl = runContext.render(this.restUrl).as(String.class).orElseThrow();
        String rJobId = runContext.render(this.jobId).as(String.class).orElseThrow();
        Boolean cancel = runContext.render(this.cancelJob).as(Boolean.class).orElse(false);

        logger.info("Triggering savepoint for job: {} (cancel: {})", rJobId, cancel);

        HttpClient client = HttpClient.builder()
            .runContext(runContext)
            .build();

        // Trigger the savepoint
        String requestId = triggerSavepoint(runContext, client, rRestUrl, rJobId, cancel);

        // Wait for completion
        String savepointPath = waitForSavepointCompletion(runContext, client, rRestUrl, rJobId, requestId);

        logger.info("Savepoint created successfully at: {}", savepointPath);

        return Output.builder()
            .jobId(rJobId)
            .savepointPath(savepointPath)
            .requestId(requestId)
            .build();
    }

    private String triggerSavepoint(RunContext runContext, HttpClient client, String restUrl, String jobId, boolean cancelJob)
            throws Exception {

        ObjectNode payload = JSON.createObjectNode();

        if (targetDirectory != null) {
            String directory = runContext.render(targetDirectory).as(String.class).orElse(null);
            if (directory != null) {
                payload.put("target-directory", directory);
            }
        }

        String format = runContext.render(formatType).as(String.class).orElse("CANONICAL");
        payload.put("format-type", format);

        payload.put("cancel-job", cancelJob);

        String body = JSON.writeValueAsString(payload);

        HttpRequest request = HttpRequest.builder()
            .uri(URI.create(restUrl + "/v1/jobs/" + jobId + "/savepoints"))
            .method("POST")
            .addHeader("Content-Type", "application/json")
            .body(HttpRequest.StringRequestBody.builder()
                .content(body)
                .build())
            .build();

        HttpResponse<String> response = client.request(request, String.class);

        if (response.getStatus().getCode() != 202) {
            throw new RuntimeException("Failed to trigger savepoint: " + response.getStatus().getCode() + " - " + response.getBody());
        }

        // Extract and return request ID
        return extractRequestIdFromResponse(response.getBody());
    }

    private String waitForSavepointCompletion(RunContext runContext, HttpClient client, String restUrl,
                                            String jobId, String requestId) throws Exception {

        int timeoutSeconds = Math.max(1, runContext.render(savepointTimeout).as(Integer.class).orElse(300));
        final int intervalSec = 5;
        final int maxAttempts = Math.max(1, (int) Math.ceil((double) timeoutSeconds / intervalSec)); // ceil, clamp ≥1
        final java.time.Instant deadline = java.time.Instant.now().plusSeconds(timeoutSeconds);

        return RetryUtils.<String, Exception>of(
            Exponential.builder()
                .delayFactor(1.0) // Fixed interval
                .interval(Duration.ofSeconds(intervalSec))
                .maxInterval(Duration.ofSeconds(intervalSec))
                .maxAttempts(maxAttempts)
                .build()
        ).run(
            (result, throwable) -> {
                if (result != null) return false;
                if (throwable instanceof NonRetriableSavepointException ||
                    throwable instanceof java.util.concurrent.TimeoutException) {
                    return false;
                }
                return true; // retry other exceptions (I/O, timeouts, transient HTTP)
            },
            () -> {
                // Hard-stop if global timeout elapsed (enforces wall-clock budget)
                if (java.time.Instant.now().isAfter(deadline)) {
                    throw new java.util.concurrent.TimeoutException("Timed out waiting for savepoint completion after " + timeoutSeconds + "s");
                }

                HttpRequest request = HttpRequest.builder()
                    .uri(URI.create(restUrl + "/v1/jobs/" + jobId + "/savepoints/" + requestId))
                    .method("GET")
                    .build();

                HttpResponse<String> response = client.request(request, String.class);

                int statusCode = response.getStatus().getCode();
                if (statusCode != 200) {
                    if (statusCode >= 500 || statusCode == 429) {
                        runContext.logger().warn("Transient status {} from Flink; will retry. Body: {}", statusCode, response.getBody());
                        return null; // keep polling
                    }
                    throw new NonRetriableSavepointException("Failed to check savepoint status: " + statusCode + " - " + response.getBody());
                }

                String status = extractSavepointStatusFromResponse(response.getBody());
                runContext.logger().debug("Savepoint status: {}", status);

                if ("COMPLETED".equals(status)) {
                    return extractSavepointPathFromResponse(response.getBody());
                } else if ("FAILED".equals(status)) {
                    String error = extractSavepointErrorFromResponse(response.getBody());
                    throw new NonRetriableSavepointException("Savepoint failed: " + error);
                }

                // Return null to continue polling
                return null;
            }
        );
    }

    private String extractRequestIdFromResponse(String responseBody) {
        try {
            JsonNode root = JSON.readTree(responseBody);
            JsonNode requestId = root.path("request-id");
            if (requestId.isTextual()) {
                return requestId.asText();
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse savepoint trigger response: " + responseBody, e);
        }
        throw new RuntimeException("Could not extract request ID from response: " + responseBody);
    }

    private String extractSavepointStatusFromResponse(String responseBody) {
        try {
            JsonNode root = JSON.readTree(responseBody);
            JsonNode status = root.path("status").path("id");
            if (status.isTextual()) {
                return status.asText();
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse savepoint status response: " + responseBody, e);
        }
        return "UNKNOWN";
    }

    private String extractSavepointPathFromResponse(String responseBody) {
        try {
            JsonNode root = JSON.readTree(responseBody);
            JsonNode location = root.path("operation").path("location");
            if (location.isTextual()) {
                return location.asText();
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse savepoint status response: " + responseBody, e);
        }
        throw new RuntimeException("Could not extract savepoint path from response: " + responseBody);
    }

    private String extractSavepointErrorFromResponse(String responseBody) {
        try {
            JsonNode root = JSON.readTree(responseBody);
            JsonNode failure = root.path("operation").path("failure-cause");
            if (failure.isObject()) {
                JsonNode message = failure.path("message");
                if (message.isTextual()) {
                    return message.asText();
                }
                JsonNode className = failure.path("class-name");
                if (className.isTextual()) {
                    return className.asText();
                }
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse savepoint status response: " + responseBody, e);
        }
        return "Unknown error";
    }

    private static final class NonRetriableSavepointException extends RuntimeException {
        NonRetriableSavepointException(String message) { super(message); }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Job ID",
            description = "ID of the Flink job for which the savepoint was created."
        )
        private final String jobId;

        @Schema(
            title = "Savepoint path",
            description = "Path returned by Flink for the created savepoint."
        )
        private final String savepointPath;

        @Schema(
            title = "Request ID",
            description = "Savepoint request ID used for status polling."
        )
        private final String requestId;
    }
}
