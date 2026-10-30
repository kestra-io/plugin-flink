package io.kestra.plugin.flink;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.flink.api.common.JobID;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.retrys.Constant;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
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
    title = "Cancel a running Flink job",
    description = "Stops a Flink job via the REST API, optionally creating a savepoint first. Supports draining streaming jobs and waits until the cancellation completes or times out."
)
@Plugin(
    examples = {
        @Example(
            title = "Cancel a job with savepoint",
            full = true,
            code = """
                id: cancel-flink-job
                namespace: company.team

                inputs:
                  - id: jobId
                    type: STRING
                    required: true

                tasks:
                  - id: cancel-job
                    type: io.kestra.plugin.flink.CancelJob
                    restUrl: "http://flink-jobmanager:8081"
                    jobId: "{{ inputs.jobId }}"
                    withSavepoint: true
                    savepointDir: "s3://flink/savepoints/canceled/{{ execution.id }}"
                    drainJob: true

                # jobId must be a valid Flink JobID and the Flink JobManager must be reachable.
                # The savepoint path requires Flink filesystem support and storage access permissions.
                """
        ),
        @Example(
            title = "Force cancel without savepoint",
            full = true,
            code = """
                id: force-cancel-job
                namespace: company.team

                inputs:
                  - id: jobId
                    type: STRING
                    required: true

                tasks:
                  - id: force-cancel
                    type: io.kestra.plugin.flink.CancelJob
                    restUrl: "http://flink-jobmanager:8081"
                    jobId: "{{ inputs.jobId }}"
                    withSavepoint: false

                # jobId must be a valid Flink JobID and the Flink JobManager must be reachable.
                """
        )
    }
)
public class CancelJob extends Task implements RunnableTask<CancelJob.Output> {
    private static final ObjectMapper JSON = JacksonMapper.ofJson();
    private static final int POLL_INTERVAL_SECONDS = 1;
    private static final String JOB_CANCELLATION_TIMEOUT_MESSAGE = "Job cancellation timed out";

    @Schema(
        title = "Flink REST API URL",
        description = "Base URL of the Flink REST API (e.g., `http://flink-jobmanager:8081`)."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> restUrl;

    @Schema(
        title = "Job ID",
        description = "ID of the Flink job to cancel."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> jobId;

    @Schema(
        title = "Create savepoint before cancellation",
        description = "Trigger a savepoint before cancelling the job; defaults to false."
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<Boolean> withSavepoint = Property.ofValue(false);

    @Schema(
        title = "Savepoint directory",
        description = "Target directory for the savepoint; required when withSavepoint is true."
    )
    @PluginProperty(group = "destination")
    private Property<String> savepointDir;

    @Schema(
        title = "Drain job",
        description = "Drain streaming input before stopping. Applicable to streaming jobs only; defaults to false."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> drainJob = Property.ofValue(false);

    @Schema(
        title = "Cancellation timeout",
        description = "Maximum polling time for cancellation and savepoint completion in seconds; in-flight REST request timeouts follow the Kestra HTTP client. Defaults to 60."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> cancellationTimeout = Property.ofValue(60);

    @Override
    public CancelJob.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        var rRestUrl = FlinkRestUrl.normalize(runContext.render(this.restUrl).as(String.class).orElseThrow());
        String rJobId = runContext.render(this.jobId).as(String.class).orElseThrow();
        Boolean withSp = runContext.render(this.withSavepoint).as(Boolean.class).orElse(false);
        Boolean drain = runContext.render(this.drainJob).as(Boolean.class).orElse(false);
        var timeoutSeconds = Math.max(1, runContext.render(cancellationTimeout).as(Integer.class).orElse(60));

        JobID.fromHexString(rJobId);
        var savepointDirectory = withSp ? getSavepointDirectory(runContext) : null;
        var deadline = Instant.now().plusSeconds(timeoutSeconds);

        logger.info("Cancelling Flink job: {} (withSavepoint: {}, drain: {})", rJobId, withSp, drain);

        try (HttpClient client = HttpClient.builder().runContext(runContext).build()) {
            String savepointPath = null;

            if (drain) {
                var stoppedWithSavepoint = stopWithSavepoint(
                    runContext,
                    client,
                    rRestUrl,
                    rJobId,
                    savepointDirectory,
                    deadline
                );
                if (withSp) {
                    savepointPath = stoppedWithSavepoint;
                }
                logger.info("Successfully stopped job: {}", rJobId);
                return Output.builder()
                    .jobId(rJobId)
                    .savepointPath(savepointPath)
                    .cancellationResult("Job stop requested (drain mode)")
                    .build();
            }

            if (withSp) {
                savepointPath = cancelWithSavepoint(
                    runContext,
                    client,
                    rRestUrl,
                    rJobId,
                    savepointDirectory,
                    deadline
                );
                logger.info("Savepoint created at: {}", savepointPath);
            } else {
                cancelJob(client, rRestUrl, rJobId);
                waitForJobCancellation(runContext, client, rRestUrl, rJobId, deadline, timeoutSeconds);
            }

            logger.info("Successfully cancelled job: {}", rJobId);

            return Output.builder()
                .jobId(rJobId)
                .savepointPath(savepointPath)
                .cancellationResult("Job cancellation requested")
                .build();
        }
    }

    private String cancelWithSavepoint(
        RunContext runContext,
        HttpClient client,
        String restUrl,
        String jobId,
        String savepointDirectory,
        Instant deadline) throws Exception {
        var payload = new LinkedHashMap<String, Object>();
        payload.put("target-directory", savepointDirectory);
        payload.put("cancel-job", true);
        payload.put("formatType", "CANONICAL");
        var requestId = triggerOperation(
            client,
            FlinkRestUrl.endpoint(restUrl, "/v1/jobs/" + jobId + "/savepoints"),
            payload,
            "savepoint cancellation"
        );
        return waitForSavepoint(runContext, client, restUrl, jobId, requestId, deadline);
    }

    private String stopWithSavepoint(
        RunContext runContext,
        HttpClient client,
        String restUrl,
        String jobId,
        String savepointDirectory,
        Instant deadline) throws Exception {
        var payload = new LinkedHashMap<String, Object>();
        if (savepointDirectory != null) {
            payload.put("targetDirectory", savepointDirectory);
        }
        payload.put("drain", true);
        payload.put("formatType", "CANONICAL");
        var requestId = triggerOperation(
            client,
            FlinkRestUrl.endpoint(restUrl, "/v1/jobs/" + jobId + "/stop"),
            payload,
            "draining stop"
        );
        return waitForSavepoint(runContext, client, restUrl, jobId, requestId, deadline);
    }

    private String triggerOperation(HttpClient client, String endpoint, Map<String, Object> payload, String operation)
        throws Exception {
        var request = HttpRequest.builder()
            .uri(URI.create(endpoint))
            .method("POST")
            .addHeader("Content-Type", "application/json")
            .body(HttpRequest.StringRequestBody.builder().content(JSON.writeValueAsString(payload)).build())
            .build();
        var response = client.request(request, String.class);
        if (response.getStatus().getCode() != 202) {
            throw new RuntimeException(
                "Failed to trigger " + operation + ": " + response.getStatus().getCode() + " - " + response.getBody()
            );
        }
        return extractRequestId(response.getBody());
    }

    private String waitForSavepoint(
        RunContext runContext,
        HttpClient client,
        String restUrl,
        String jobId,
        String requestId,
        Instant deadline) throws Exception {
        var timeoutSeconds = remainingSeconds(deadline);
        var maxAttempts = maxAttemptsForTimeout(timeoutSeconds);
        String savepointPath;
        try {
            savepointPath = RetryUtils.<String, Exception> of(
                Constant.builder()
                    .interval(Duration.ofSeconds(POLL_INTERVAL_SECONDS))
                    .maxAttempts(maxAttempts)
                    .build()
            ).run(
                (result, throwable) -> result == null && (throwable == null || isTransientFailure(throwable)),
                () ->
                {
                    ensureBeforeDeadline(deadline, timeoutSeconds);
                    var response = requestStatus(
                        client,
                        FlinkRestUrl.endpoint(restUrl, "/v1/jobs/" + jobId + "/savepoints/" + requestId),
                        "savepoint"
                    );
                    var statusCode = response.getStatus().getCode();
                    if (statusCode == 429 || statusCode >= 500) {
                        runContext.logger().warn("Transient status {} from Flink; retrying savepoint status poll", statusCode);
                        return null;
                    }
                    if (statusCode != 200) {
                        throw new NonRetriableCancellationException(
                            "Failed to check savepoint status: " + statusCode + " - " + response.getBody()
                        );
                    }

                    var root = JSON.readTree(response.getBody());
                    var state = root.path("status").path("id").asText();
                    if ("COMPLETED".equals(state)) {
                        var path = root.path("operation").path("location").asText(null);
                        if (path == null) {
                            throw new IllegalStateException("Flink completed the savepoint without returning its path");
                        }
                        return path;
                    }
                    if ("FAILED".equals(state)) {
                        var failure = root.path("operation").path("failure-cause").path("message").asText("Unknown error");
                        throw new NonRetriableCancellationException("Savepoint failed: " + failure);
                    }
                    return null;
                }
            );
        } catch (RetryUtils.RetryFailed e) {
            throw new CancellationTimeoutException(JOB_CANCELLATION_TIMEOUT_MESSAGE, e);
        }
        if (savepointPath == null) {
            throw new CancellationTimeoutException(JOB_CANCELLATION_TIMEOUT_MESSAGE);
        }
        return savepointPath;
    }

    private void cancelJob(HttpClient client, String restUrl, String jobId) throws Exception {
        var request = HttpRequest.builder()
            .uri(URI.create(FlinkRestUrl.endpoint(restUrl, "/v1/jobs/" + jobId)))
            .method("PATCH")
            .build();
        var response = client.request(request, String.class);
        if (response.getStatus().getCode() != 202) {
            throw new RuntimeException(
                "Failed to cancel job: " + response.getStatus().getCode() + " - " + response.getBody()
            );
        }
    }

    private void waitForJobCancellation(
        RunContext runContext,
        HttpClient client,
        String restUrl,
        String jobId,
        Instant deadline,
        int timeoutSeconds) throws Exception {
        var maxAttempts = maxAttemptsForTimeout(timeoutSeconds);
        Boolean completed;
        try {
            completed = RetryUtils.<Boolean, Exception> of(
                Constant.builder()
                    .interval(Duration.ofSeconds(POLL_INTERVAL_SECONDS))
                    .maxAttempts(maxAttempts)
                    .build()
            ).run(
                (result, throwable) -> result == null && (throwable == null || isTransientFailure(throwable)),
                () ->
                {
                    ensureBeforeDeadline(deadline, timeoutSeconds);
                    var response = requestStatus(
                        client,
                        FlinkRestUrl.endpoint(restUrl, "/v1/jobs/" + jobId),
                        "job"
                    );
                    var statusCode = response.getStatus().getCode();
                    if (statusCode == 404) {
                        return true;
                    }
                    if (statusCode == 429 || statusCode >= 500) {
                        runContext.logger().warn("Transient status {} from Flink; retrying cancellation status poll", statusCode);
                        return null;
                    }
                    if (statusCode != 200) {
                        throw new NonRetriableCancellationException(
                            "Failed to check job status: " + statusCode + " - " + response.getBody()
                        );
                    }
                    var state = JSON.readTree(response.getBody()).path("state").asText();
                    if ("FINISHED".equals(state) || "FAILED".equals(state) || "CANCELED".equals(state)) {
                        return true;
                    }
                    return null;
                }
            );
        } catch (RetryUtils.RetryFailed e) {
            throw new CancellationTimeoutException(JOB_CANCELLATION_TIMEOUT_MESSAGE, e);
        }
        if (!Boolean.TRUE.equals(completed)) {
            throw new CancellationTimeoutException(JOB_CANCELLATION_TIMEOUT_MESSAGE);
        }
    }

    private HttpResponse<String> requestStatus(HttpClient client, String endpoint, String operation) throws Exception {
        var request = HttpRequest.builder()
            .uri(URI.create(endpoint))
            .method("GET")
            .build();
        try {
            return client.request(request, String.class);
        } catch (HttpClientResponseException e) {
            var statusCode = e.getResponse() == null ? -1 : e.getResponse().getStatus().getCode();
            if (statusCode == 404 && "job".equals(operation)) {
                return HttpResponse.<String> of(HttpResponse.Status.NOT_FOUND, null);
            }
            if (statusCode == 429 || statusCode >= 500) {
                return HttpResponse.<String> of(HttpResponse.Status.valueOf(statusCode), null);
            }
            throw e;
        }
    }

    private String getSavepointDirectory(RunContext runContext) throws Exception {
        var savepointDirectory = savepointDir != null
            ? runContext.render(savepointDir).as(String.class).orElse(null)
            : null;
        if (savepointDirectory == null || savepointDirectory.isBlank()) {
            throw new IllegalArgumentException("savepointDir is required when withSavepoint is true");
        }
        return savepointDirectory;
    }

    private int remainingSeconds(Instant deadline) throws TimeoutException {
        var remaining = Duration.between(Instant.now(), deadline).getSeconds();
        if (remaining <= 0) {
            throw new CancellationTimeoutException(JOB_CANCELLATION_TIMEOUT_MESSAGE);
        }
        return (int) Math.min(Integer.MAX_VALUE, remaining + 1);
    }

    private int maxAttemptsForTimeout(int timeoutSeconds) {
        return timeoutSeconds == Integer.MAX_VALUE ? Integer.MAX_VALUE : timeoutSeconds + 1;
    }

    private void ensureBeforeDeadline(Instant deadline, int timeoutSeconds) throws TimeoutException {
        if (!Instant.now().isBefore(deadline)) {
            throw new CancellationTimeoutException(
                JOB_CANCELLATION_TIMEOUT_MESSAGE + " after " + timeoutSeconds + " seconds"
            );
        }
    }

    private boolean isTransientFailure(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof CancellationTimeoutException) {
                return false;
            }
            if (cause instanceof IOException || cause instanceof TimeoutException) {
                return true;
            }
            if (cause instanceof HttpClientResponseException responseException && responseException.getResponse() != null) {
                var statusCode = responseException.getResponse().getStatus().getCode();
                return statusCode == 429 || statusCode >= 500;
            }
        }
        return false;
    }

    private String extractRequestId(String responseBody) throws JsonProcessingException {
        var requestId = JSON.readTree(responseBody).path("request-id").asText(null);
        if (requestId == null) {
            throw new IllegalStateException("Could not extract request ID from Flink response: " + responseBody);
        }
        return requestId;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Cancelled job ID",
            description = "ID of the Flink job that was cancelled."
        )
        private final String jobId;

        @Schema(
            title = "Savepoint path",
            description = "Path to the savepoint created before cancellation when requested."
        )
        private final String savepointPath;

        @Schema(
            title = "Cancellation result",
            description = "Result message returned by Flink for the cancellation request."
        )
        private final String cancellationResult;

    }

    private static final class NonRetriableCancellationException extends RuntimeException {
        NonRetriableCancellationException(String message) {
            super(message);
        }
    }

    private static final class CancellationTimeoutException extends TimeoutException {
        private CancellationTimeoutException(String message) {
            super(message);
        }

        private CancellationTimeoutException(String message, Throwable cause) {
            super(message);
            initCause(cause);
        }
    }
}
