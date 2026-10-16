package io.kestra.plugin.flink;

import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.RetryUtils;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

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

                inputs:
                  - id: jobId
                    type: STRING
                    required: true

                tasks:
                  - id: trigger-savepoint
                    type: io.kestra.plugin.flink.TriggerSavepoint
                    restUrl: "http://flink-jobmanager:8081"
                    jobId: "{{ inputs.jobId }}"
                    targetDirectory: "s3://flink/savepoints/backup/{{ execution.id }}"
                    savepointTimeout: 300

                # jobId must identify an existing Flink job and the Flink JobManager must be reachable.
                # The cluster must support the target filesystem and have access permissions for this path.
                """
        ),
        @Example(
            title = "Trigger savepoint with default directory",
            full = true,
            code = """
                id: default-savepoint
                namespace: company.team

                inputs:
                  - id: jobId
                    type: STRING
                    required: true

                tasks:
                  - id: trigger-savepoint
                    type: io.kestra.plugin.flink.TriggerSavepoint
                    restUrl: "http://flink-jobmanager:8081"
                    jobId: "{{ inputs.jobId }}"

                # jobId must identify an existing Flink job and the Flink JobManager must be reachable.
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
    @PluginProperty(group = "main")
    private Property<String> restUrl;

    @Schema(
        title = "Job ID",
        description = "ID of the Flink job to snapshot."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> jobId;

    @Schema(
        title = "Target directory",
        description = "Target directory for the savepoint; falls back to the cluster default when omitted."
    )
    @PluginProperty(group = "destination")
    private Property<String> targetDirectory;

    @Schema(
        title = "Cancel job after savepoint",
        description = "Cancel the job after the savepoint completes; defaults to false."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> cancelJob = Property.ofValue(false);

    @Schema(
        title = "Savepoint timeout",
        description = "Maximum wait time for savepoint completion in seconds; defaults to 300."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> savepointTimeout = Property.ofValue(300);

    @Schema(
        title = "Format type",
        description = "Savepoint format type: CANONICAL or NATIVE. Defaults to CANONICAL for broader compatibility."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<String> formatType = Property.ofValue("CANONICAL");

    @Override
    public TriggerSavepoint.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rRestUrl = normalizeRestUrl(runContext.render(this.restUrl).as(String.class).orElseThrow());
        String rJobId = runContext.render(this.jobId).as(String.class).orElseThrow();
        Boolean cancel = runContext.render(this.cancelJob).as(Boolean.class).orElse(false);

        logger.info("Triggering savepoint for job: {} (cancel: {})", rJobId, cancel);

        String requestId;
        String savepointPath;
        try (
            RestClusterClient<StandaloneClusterId> flinkClient = new RestClusterClient<>(
                createConfiguration(URI.create(rRestUrl)),
                StandaloneClusterId.getInstance()
            );
            HttpClient client = HttpClient.builder()
                .runContext(runContext)
                .build()
        ) {
            requestId = triggerSavepoint(runContext, flinkClient, rJobId, cancel);

            // Wait for completion
            savepointPath = waitForSavepointCompletion(runContext, client, rRestUrl, rJobId, requestId);
        }

        logger.info("Savepoint created successfully at: {}", savepointPath);

        return Output.builder()
            .jobId(rJobId)
            .savepointPath(savepointPath)
            .requestId(requestId)
            .build();
    }

    private String triggerSavepoint(RunContext runContext, RestClusterClient<StandaloneClusterId> client, String jobId, boolean cancelJob)
        throws Exception {
        String directory = targetDirectory == null
            ? null
            : runContext.render(targetDirectory).as(String.class).orElse(null);
        String format = runContext.render(formatType).as(String.class).orElse("CANONICAL");
        SavepointFormatType formatType = SavepointFormatType.valueOf(format.toUpperCase(Locale.ROOT));
        JobID flinkJobId = JobID.fromHexString(jobId);
        if (!cancelJob) {
            return await(client.triggerDetachedSavepoint(flinkJobId, directory, formatType));
        }

        SavepointTriggerHeaders headers = SavepointTriggerHeaders.getInstance();
        SavepointTriggerMessageParameters parameters = headers.getUnresolvedMessageParameters();
        parameters.jobID.resolve(flinkJobId);
        try {
            CompletableFuture<TriggerResponse> response = client.sendRequest(
                headers,
                parameters,
                new SavepointTriggerRequestBody(directory, cancelJob, formatType, null)
            );
            return response.get().getTriggerId().toString();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new RuntimeException("Flink savepoint trigger failed", cause);
        }
    }

    private Configuration createConfiguration(URI restUri) {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.ADDRESS, restUri.getHost());
        configuration.set(RestOptions.PORT, restUri.getPort() < 0 ? RestOptions.PORT.defaultValue() : restUri.getPort());

        String path = restUri.getPath();
        if (path != null) {
            path = path.replaceFirst("/+$", "");
        }
        if (path != null && !path.isEmpty()) {
            configuration.set(RestOptions.PATH, path);
        }
        if ("https".equalsIgnoreCase(restUri.getScheme())) {
            configuration.set(SecurityOptions.SSL_REST_ENABLED, true);
        }
        return configuration;
    }

    private <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new RuntimeException("Flink savepoint trigger failed", cause);
        }
    }

    private String waitForSavepointCompletion(RunContext runContext, HttpClient client, String restUrl,
        String jobId, String requestId) throws Exception {

        int timeoutSeconds = Math.max(1, runContext.render(savepointTimeout).as(Integer.class).orElse(300));
        final int intervalSec = 5;
        final int maxAttempts = Math.max(1, (int) Math.ceil((double) timeoutSeconds / intervalSec)); // ceil, clamp ≥1
        final java.time.Instant deadline = java.time.Instant.now().plusSeconds(timeoutSeconds);

        return RetryUtils.<String, Exception> of(
            Exponential.builder()
                .delayFactor(1.0001)
                .interval(Duration.ofSeconds(intervalSec))
                .maxInterval(Duration.ofSeconds(intervalSec + 1L))
                .maxAttempts(maxAttempts)
                .build()
        ).run(
            (result, throwable) ->
            {
                if (result != null)
                    return false;
                if (
                    throwable instanceof NonRetriableSavepointException ||
                        throwable instanceof java.util.concurrent.TimeoutException
                ) {
                    return false;
                }
                return true; // retry other exceptions (I/O, timeouts, transient HTTP)
            },
            () ->
            {
                // Hard-stop if global timeout elapsed (enforces wall-clock budget)
                if (java.time.Instant.now().isAfter(deadline)) {
                    throw new java.util.concurrent.TimeoutException("Timed out waiting for savepoint completion after " + timeoutSeconds + "s");
                }

                HttpRequest request = HttpRequest.builder()
                    .uri(URI.create(restEndpoint(restUrl, "/v1/jobs/" + jobId + "/savepoints/" + requestId)))
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

    private String normalizeRestUrl(String restUrl) {
        URI restUri = URI.create(restUrl);
        String scheme = restUri.getScheme();
        if (restUri.getHost() == null || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException("restUrl must be an HTTP or HTTPS URL with a host: " + restUrl);
        }
        if (restUri.getRawQuery() != null || restUri.getRawFragment() != null) {
            throw new IllegalArgumentException("restUrl must not include query parameters or a fragment: " + restUrl);
        }
        if (restUri.getPort() >= 0) {
            return restUrl;
        }

        StringBuilder normalizedUrl = new StringBuilder()
            .append(scheme)
            .append("://")
            .append(restUri.getRawAuthority())
            .append(':')
            .append(RestOptions.PORT.defaultValue());
        String path = restUri.getRawPath();
        if (path != null) {
            path = path.replaceFirst("/+$", "");
            normalizedUrl.append(path);
        }
        return normalizedUrl.toString();
    }

    private String restEndpoint(String restUrl, String endpoint) {
        return restUrl.replaceFirst("/+$", "") + endpoint;
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
        NonRetriableSavepointException(String message) {
            super(message);
        }
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
