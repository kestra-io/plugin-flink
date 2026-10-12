package io.kestra.plugin.flink;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.slf4j.Logger;

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
        description = "Maximum time to wait for cancellation completion in seconds; defaults to 60."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> cancellationTimeout = Property.ofValue(60);

    @Override
    public CancelJob.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rRestUrl = runContext.render(this.restUrl).as(String.class).orElseThrow();
        String rJobId = runContext.render(this.jobId).as(String.class).orElseThrow();
        Boolean withSp = runContext.render(this.withSavepoint).as(Boolean.class).orElse(false);
        Boolean drain = runContext.render(this.drainJob).as(Boolean.class).orElse(false);
        int timeoutSeconds = Math.max(1, runContext.render(cancellationTimeout).as(Integer.class).orElse(60));

        logger.info("Cancelling Flink job: {} (withSavepoint: {}, drain: {})", rJobId, withSp, drain);

        JobID flinkJobId = JobID.fromHexString(rJobId);
        Configuration configuration = createConfiguration(rRestUrl);

        try (
            RestClusterClient<StandaloneClusterId> client = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance())
        ) {
            String savepointPath = null;
            boolean cancelledWithSavepoint = withSp && !drain;

            if (withSp) {
                if (cancelledWithSavepoint) {
                    savepointPath = cancelWithSavepoint(runContext, client, flinkJobId);
                    logger.info("Savepoint created at: {}", savepointPath);
                } else {
                    savepointPath = triggerSavepoint(runContext, client, flinkJobId);
                    logger.info("Savepoint created at: {}", savepointPath);
                }
            }

            String cancellationResult = "Job cancellation requested";
            if (!cancelledWithSavepoint) {
                cancellationResult = cancelJob(client, flinkJobId, drain, timeoutSeconds);
            }

            if (!drain) {
                waitForJobCancellation(runContext, client, flinkJobId, timeoutSeconds);
            }

            logger.info("Successfully cancelled job: {}", rJobId);

            return Output.builder()
                .jobId(rJobId)
                .savepointPath(savepointPath)
                .cancellationResult(cancellationResult)
                .build();
        }
    }

    private String triggerSavepoint(RunContext runContext, RestClusterClient<StandaloneClusterId> client, JobID jobId)
        throws Exception {
        String savepointDirectory = getSavepointDirectory(runContext);
        try {
            return await(client.triggerSavepoint(jobId, savepointDirectory, SavepointFormatType.DEFAULT), 300, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException("Timed out waiting for savepoint completion after 300 seconds");
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Savepoint failed: " + e.getMessage(), e);
        }
    }

    private String cancelWithSavepoint(RunContext runContext, RestClusterClient<StandaloneClusterId> client, JobID jobId)
        throws Exception {
        String savepointDirectory = getSavepointDirectory(runContext);
        try {
            return await(client.cancelWithSavepoint(jobId, savepointDirectory, SavepointFormatType.DEFAULT), 300, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException("Timed out waiting for savepoint completion after 300 seconds");
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Savepoint failed: " + e.getMessage(), e);
        }
    }

    private String getSavepointDirectory(RunContext runContext) throws Exception {
        String savepointDirectory = savepointDir != null
            ? runContext.render(savepointDir).as(String.class).orElse(null)
            : null;
        if (savepointDirectory == null) {
            throw new IllegalArgumentException("savepointDir is required when withSavepoint is true");
        }
        return savepointDirectory;
    }

    private String cancelJob(RestClusterClient<StandaloneClusterId> client, JobID jobId, boolean drain, int timeoutSeconds)
        throws Exception {
        CompletableFuture<?> future;
        if (drain) {
            future = client.stopWithSavepoint(jobId, true, null, SavepointFormatType.DEFAULT);
        } else {
            future = client.cancel(jobId);
        }

        try {
            if (drain) {
                await(future, timeoutSeconds, TimeUnit.SECONDS);
            } else {
                await(future);
            }
        } catch (TimeoutException e) {
            throw new TimeoutException("Job cancellation timed out after " + timeoutSeconds + " seconds");
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to cancel job: " + e.getMessage(), e);
        }

        return drain ? "Job stop requested (drain mode)" : "Job cancellation requested";
    }

    private void waitForJobCancellation(
        RunContext runContext,
        RestClusterClient<StandaloneClusterId> client,
        JobID jobId,
        int timeoutSeconds) throws Exception {
        final int intervalSec = 2;
        final int maxAttempts = Math.max(1, (int) Math.ceil((double) timeoutSeconds / intervalSec));
        final java.time.Instant deadline = java.time.Instant.now().plusSeconds(timeoutSeconds);

        RetryUtils.<Boolean, Exception> of(
            Exponential.builder()
                .delayFactor(1.0001)
                .interval(Duration.ofSeconds(intervalSec))
                .maxInterval(Duration.ofSeconds(intervalSec + 1L))
                .maxAttempts(maxAttempts)
                .build()
        ).run(
            (result, throwable) ->
            {
                if (result != null && result)
                    return false;
                if (
                    throwable instanceof TimeoutException ||
                        throwable instanceof InterruptedException
                ) {
                    return false;
                }
                return true; // retry other exceptions
            },
            () ->
            {
                // Hard-stop if global timeout elapsed
                if (java.time.Instant.now().isAfter(deadline)) {
                    throw new java.util.concurrent.TimeoutException("Job cancellation timed out after " + timeoutSeconds + " seconds");
                }

                try {
                    JobStatus status = await(client.getJobStatus(jobId));
                    if (status == JobStatus.FINISHED || status == JobStatus.FAILED || status == JobStatus.CANCELED) {
                        return true;
                    }
                } catch (Exception e) {
                    if (isJobNotFound(e)) {
                        return true;
                    }
                    RestClientException restException = findRestClientException(e);
                    if (restException != null) {
                        int statusCode = restException.getHttpResponseStatus().code();
                        if (statusCode >= 500 || statusCode == 429) {
                            runContext.logger().warn(
                                "Transient status {} from Flink; will retry. Body: {}",
                                statusCode,
                                restException.getMessage()
                            );
                            return null;
                        }
                        throw new NonRetriableCancellationException(
                            "Failed to check job status: " + statusCode + " - " + restException.getMessage()
                        );
                    }
                    throw e;
                }

                // Return null to continue polling
                return null;
            }
        );
    }

    private Configuration createConfiguration(String restUrl) {
        URI restUri = URI.create(restUrl);
        String scheme = restUri.getScheme();
        if (restUri.getHost() == null || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException("restUrl must be an HTTP or HTTPS URL with a host: " + restUrl);
        }
        if (restUri.getRawQuery() != null || restUri.getRawFragment() != null) {
            throw new IllegalArgumentException("restUrl must not include query parameters or a fragment: " + restUrl);
        }

        Configuration configuration = new Configuration();
        configuration.set(RestOptions.ADDRESS, restUri.getHost());
        int port = restUri.getPort() < 0 ? RestOptions.PORT.defaultValue() : restUri.getPort();
        configuration.set(RestOptions.PORT, port);

        String path = restUri.getPath();
        if (path != null) {
            path = path.replaceFirst("/+$", "");
        }
        if (path != null && !path.isEmpty()) {
            configuration.set(RestOptions.PATH, path);
        }
        if ("https".equalsIgnoreCase(scheme)) {
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
            throwFutureFailure(e);
            throw e;
        }
    }

    private <T> T await(CompletableFuture<T> future, long timeout, TimeUnit unit) throws Exception {
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            throwFutureFailure(e);
            throw e;
        }
    }

    private void throwFutureFailure(ExecutionException exception) throws Exception {
        Throwable cause = exception.getCause();
        if (cause instanceof Exception) {
            throw (Exception) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        throw new RuntimeException("Flink client operation failed", cause);
    }

    private boolean isJobNotFound(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null) {
            if (cause instanceof FlinkJobNotFoundException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private RestClientException findRestClientException(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null) {
            if (cause instanceof RestClientException) {
                return (RestClientException) cause;
            }
            cause = cause.getCause();
        }
        return null;
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

}
