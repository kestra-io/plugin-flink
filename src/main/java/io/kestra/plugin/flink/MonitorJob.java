package io.kestra.plugin.flink;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.slf4j.Logger;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.PebbleUtil;

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
    title = "Trigger on Flink job state",
    description = "Polls a Flink job over REST and triggers a flow when it reaches a terminal state. Uses a 10s polling default and can fail the flow on FAILED states."
)
@Plugin(
    examples = {
        @Example(
            title = "Monitor Flink job and trigger on completion",
            full = true,
            code = """
                id: flink-job-monitor
                namespace: company.team

                inputs:
                  - id: jobId
                    type: STRING
                    required: true

                triggers:
                  - id: job-monitor
                    type: io.kestra.plugin.flink.MonitorJob
                    restUrl: "http://flink-jobmanager:8081"
                    jobId: "{{ inputs.jobId }}"
                    interval: "PT30S"
                    expectedTerminalStates:
                      - "FINISHED"
                      - "CANCELED"
                    failOnError: true

                tasks:
                  - id: notify-completion
                    type: io.kestra.plugin.core.log.Log
                    message: "Flink job {{ trigger.jobId }} reached state: {{ trigger.finalState }}"

                # jobId must be a valid hexadecimal Flink JobID for an existing job.
                # The Flink JobManager must be reachable by this trigger.
                """
        )
    }
)
public class MonitorJob extends AbstractTrigger implements PollingTriggerInterface {
    private static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(10);

    @Schema(
        title = "Flink REST API URL",
        description = "Base URL of the Flink REST API (e.g., `http://flink-jobmanager:8081`)."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> restUrl;

    @Schema(
        title = "Job ID",
        description = "ID of the Flink job to monitor."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> jobId;

    @Schema(
        title = "Polling interval",
        description = "Interval between job status checks (static ISO-8601 duration, e.g., PT30S). Defaults to PT10S."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> interval = Property.ofValue(Duration.parse("PT10S"));

    @Schema(
        title = "Fail on error",
        description = "Fail the trigger when the job reaches FAILED. If false, trigger still fires with success=false; defaults to true."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> failOnError = Property.ofValue(true);

    @Schema(
        title = "Expected terminal states",
        description = "Job states treated as success. Defaults to ['FINISHED']."
    )
    @PluginProperty(group = "execution")
    private Property<java.util.List<String>> expectedTerminalStates;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        String rRestUrl = runContext.render(this.restUrl).as(String.class).orElseThrow();
        String rJobId = runContext.render(this.jobId).as(String.class).orElseThrow();
        Boolean failOnErr = runContext.render(this.failOnError).as(Boolean.class).orElse(true);
        var flinkJobId = JobID.fromHexString(rJobId);
        var configuration = FlinkRestUrl.configuration(rRestUrl);

        logger.debug("Polling Flink job status: {}", rJobId);

        JobStatus status;
        try (
            var client = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance())
        ) {
            org.apache.flink.api.common.JobStatus flinkStatus;
            try {
                flinkStatus = client.getJobStatus(flinkJobId).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (ExecutionException e) {
                var failure = e.getCause() == null ? e : e.getCause();
                if (isJobNotFound(failure)) {
                    throw new RuntimeException("Job not found: " + rJobId, failure);
                }
                if (isTransientFailure(failure)) {
                    logger.warn("Transient failure polling Flink job {}: {}", rJobId, failure.getMessage());
                    return Optional.empty();
                }
                if (failure instanceof Exception exception) {
                    throw exception;
                }
                throw e;
            }

            var state = flinkStatus.name();
            String stateDetails = null;
            if (isTerminalState(state)) {
                try {
                    stateDetails = getStateDetails(runContext, rRestUrl, rJobId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                } catch (Exception e) {
                    logger.warn("Could not retrieve optional state details for Flink job {}: {}", rJobId, e.getMessage());
                }
            }
            status = JobStatus.builder()
                .state(state)
                .stateDetails(stateDetails)
                .build();
        }
        logger.debug("Job {} current status: {}", rJobId, status.getState());

        if (isTerminalState(status.getState())) {
            var expectedStates = getExpectedTerminalStates(runContext);
            var isSuccess = expectedStates.contains(status.getState());

            if (!isSuccess && failOnErr && "FAILED".equals(status.getState())) {
                logger.error("Job {} failed: {}", rJobId, status.getStateDetails());
            } else {
                logger.info("Job {} reached terminal state: {} (success: {})", rJobId, status.getState(), isSuccess);
            }

            var triggerOutputs = Map.<String, Object> of(
                "jobId", rJobId,
                "finalState", status.getState(),
                "stateDetails", status.getStateDetails() != null ? status.getStateDetails() : "",
                "success", isSuccess
            );

            return Optional.of(
                Execution.builder()
                    .id(IdUtils.create())
                    .namespace(conditionContext.getFlow().getNamespace())
                    .flowId(conditionContext.getFlow().getId())
                    .flowRevision(conditionContext.getFlow().getRevision())
                    .state(new State())
                    .variables(
                        Map.of(
                            "jobId", rJobId,
                            "finalState", status.getState(),
                            "stateDetails", status.getStateDetails() != null ? status.getStateDetails() : "",
                            "success", isSuccess
                        )
                    )
                    .outputs(triggerOutputs)
                    .build()
            );
        }

        return Optional.empty();
    }

    @Override
    public Duration getInterval() {
        if (interval == null) {
            return DEFAULT_INTERVAL;
        }
        var configuredInterval = interval.toString();
        if (PebbleUtil.containsOpeningBlockDelimiter(configuredInterval)) {
            return DEFAULT_INTERVAL;
        }
        try {
            return Duration.parse(configuredInterval);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("interval must be an ISO-8601 duration: " + configuredInterval, e);
        }
    }

    String getStateDetails(RunContext runContext, String restUrl, String jobId) throws Exception {
        try (
            var client = HttpClient.builder()
                .runContext(runContext)
                .build()
        ) {
            var request = HttpRequest.builder()
                .uri(URI.create(FlinkRestUrl.endpoint(restUrl, "/v1/jobs/" + jobId)))
                .method("GET")
                .build();

            var response = client.request(request, String.class);
            if (response.getStatus().getCode() == 404) {
                return null;
            }
            if (response.getStatus().getCode() != 200) {
                throw new RuntimeException(
                    "Failed to get job status: " + response.getStatus().getCode() + " - " + response.getBody()
                );
            }
            return extractJsonValue(response.getBody(), "state-details");
        }
    }

    private boolean isJobNotFound(Throwable throwable) {
        var cause = throwable;
        while (cause != null) {
            if (cause instanceof FlinkJobNotFoundException) {
                return true;
            }
            if (cause instanceof RestClientException restException && restException.getHttpResponseStatus().code() == 404) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private boolean isTransientFailure(Throwable throwable) {
        var cause = throwable;
        while (cause != null) {
            if (cause instanceof IOException || cause instanceof TimeoutException) {
                return true;
            }
            if (cause instanceof RestClientException restException) {
                var statusCode = restException.getHttpResponseStatus().code();
                return statusCode == 429 || statusCode >= 500;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private String extractJsonValue(String json, String key) {
        Matcher matcher = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"([^\"]*)\"")
            .matcher(json);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private boolean isTerminalState(String state) {
        return "FINISHED".equals(state) ||
            "CANCELED".equals(state) ||
            "FAILED".equals(state);
    }

    private java.util.List<String> getExpectedTerminalStates(RunContext runContext)
        throws IllegalVariableEvaluationException {
        if (expectedTerminalStates != null) {
            return runContext.render(expectedTerminalStates).asList(String.class);
        }
        return java.util.Arrays.asList("FINISHED");
    }

    @Builder
    @Getter
    public static class JobStatus {
        private final String state;
        private final String stateDetails;
    }

}
