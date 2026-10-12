package io.kestra.plugin.flink;

import java.net.URI;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.slf4j.Logger;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
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

        logger.debug("Polling Flink job status: {}", rJobId);

        try {
            Configuration configuration = createConfiguration(rRestUrl);
            JobID flinkJobId = JobID.fromHexString(rJobId);
            JobStatus status;
            try (
                RestClusterClient<StandaloneClusterId> client = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance())
            ) {
                org.apache.flink.api.common.JobStatus flinkStatus;
                try {
                    flinkStatus = client.getJobStatus(flinkJobId).get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }

                String state = flinkStatus.name();
                String stateDetails = isTerminalState(state) ? getStateDetails(runContext, rRestUrl, rJobId) : null;
                status = JobStatus.builder()
                    .state(state)
                    .stateDetails(stateDetails)
                    .build();
            }
            logger.debug("Job {} current status: {}", rJobId, status.getState());

            if (isTerminalState(status.getState())) {
                // Check if this is a successful completion
                java.util.List<String> expectedStates = getExpectedTerminalStates(runContext);
                boolean isSuccess = expectedStates.contains(status.getState());

                if (!isSuccess && failOnErr && "FAILED".equals(status.getState())) {
                    logger.error("Job {} failed: {}", rJobId, status.getStateDetails());
                } else {
                    logger.info("Job {} reached terminal state: {} (success: {})", rJobId, status.getState(), isSuccess);
                }

                // Create execution with job status information and trigger outputs
                Map<String, Object> triggerOutputs = Map.of(
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

            // Job is not in terminal state, continue polling
            return Optional.empty();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (Exception e) {
            if (isJobNotFound(e)) {
                throw new RuntimeException("Job not found: " + rJobId, e);
            }
            logger.error("Failed to check job status for {}", rJobId, e);
            // For transient errors (IOException, timeouts), continue polling
            // For permanent errors (404, invalid URL), fail fast
            if (e instanceof RuntimeException && e.getMessage() != null && e.getMessage().contains("Job not found")) {
                throw e; // Fail fast on permanent errors
            }
            return Optional.empty(); // Retry on transient errors
        }
    }

    @Override
    public Duration getInterval() {
        if (interval == null) {
            return Duration.parse("PT10S");
        }
        String configuredInterval = interval.toString();
        if (configuredInterval.contains("{")) {
            return Duration.parse("PT10S");
        }
        try {
            return Duration.parse(configuredInterval);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("interval must be an ISO-8601 duration: " + configuredInterval, e);
        }
    }

    String getStateDetails(RunContext runContext, String restUrl, String jobId) throws Exception {
        try (
            HttpClient client = HttpClient.builder()
                .runContext(runContext)
                .build()
        ) {
            HttpRequest request = HttpRequest.builder()
                .uri(URI.create(restEndpoint(restUrl, "/v1/jobs/" + jobId)))
                .method("GET")
                .build();

            HttpResponse<String> response = client.request(request, String.class);
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

    private Configuration createConfiguration(String restUrl) {
        URI restUri = validateRestUrl(restUrl);
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

    private String restEndpoint(String restUrl, String endpoint) {
        URI restUri = validateRestUrl(restUrl);
        String baseUrl = restUrl;
        if (restUri.getPort() < 0) {
            String path = restUri.getRawPath();
            baseUrl = restUri.getScheme() + "://" + restUri.getRawAuthority() + ":" + RestOptions.PORT.defaultValue()
                + (path == null ? "" : path);
        }
        return baseUrl.replaceFirst("/+$", "") + endpoint;
    }

    private URI validateRestUrl(String restUrl) {
        URI restUri = URI.create(restUrl);
        String scheme = restUri.getScheme();
        if (restUri.getHost() == null || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException("restUrl must be an HTTP or HTTPS URL with a host: " + restUrl);
        }
        if (restUri.getRawQuery() != null || restUri.getRawFragment() != null) {
            throw new IllegalArgumentException("restUrl must not include query parameters or a fragment: " + restUrl);
        }
        return restUri;
    }

    private boolean isJobNotFound(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null) {
            if (cause instanceof FlinkJobNotFoundException) {
                return true;
            }
            if (cause instanceof RestClientException restException && restException.getHttpResponseStatus().code() == 404) {
                return true;
            }
            if (cause.getMessage() != null && cause.getMessage().contains("Job not found")) {
                return true;
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
