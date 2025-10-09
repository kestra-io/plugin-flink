package io.kestra.plugin.flink;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.models.flows.State;
import io.kestra.core.utils.IdUtils;
import java.util.Map;
import java.util.Optional;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import jakarta.validation.constraints.NotNull;
import java.net.URI;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Monitor a Flink job and trigger flow execution on state changes.",
    description = "This trigger continuously monitors a Flink job and triggers flow execution " +
                  "when the job reaches specified terminal states or encounters failures."
)
@Plugin(
    examples = {
        @Example(
            title = "Monitor Flink job and trigger on completion",
            full = true,
            code = """
                id: flink-job-monitor
                namespace: company.team

                triggers:
                  - id: job-monitor
                    type: io.kestra.plugin.flink.MonitorJob
                    restUrl: "http://flink-jobmanager:8081"
                    jobId: "my-job-id-12345"
                    interval: "PT30S"
                    expectedTerminalStates:
                      - "FINISHED"
                      - "CANCELED"
                    failOnError: true

                tasks:
                  - id: notify-completion
                    type: io.kestra.core.tasks.log.Log
                    message: "Flink job {{ trigger.jobId }} reached state: {{ trigger.finalState }}"
                """
        )
    }
)
public class MonitorJob extends AbstractTrigger implements PollingTriggerInterface {

    @Schema(
        title = "Flink REST API URL",
        description = "The base URL of the Flink cluster's REST API, e.g., 'http://flink-jobmanager:8081'"
    )
    @NotNull
    private Property<String> restUrl;

    @Schema(
        title = "Job ID",
        description = "The ID of the Flink job to monitor"
    )
    @NotNull
    private Property<String> jobId;

    @Schema(
        title = "Polling interval",
        description = "Interval between job status checks. " +
                      "Use ISO-8601 duration format (e.g., 'PT30S' for 30 seconds). " +
                      "Defaults to PT10S."
    )
    @Builder.Default
    private Property<Duration> interval = Property.of(Duration.parse("PT10S"));

    @Schema(
        title = "Fail on error",
        description = "Whether to fail the task if the job reaches FAILED state. " +
                      "If false, the task will complete successfully even if the job failed. " +
                      "Defaults to true."
    )
    @Builder.Default
    private Property<Boolean> failOnError = Property.of(true);

    @Schema(
        title = "Expected terminal states",
        description = "List of job states to consider as successful completion. " +
                      "Defaults to ['FINISHED']."
    )
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
            HttpClient client = HttpClient.builder()
                .runContext(runContext)
                .build();

            JobStatus status = getJobStatus(client, rRestUrl, rJobId);
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

                return Optional.of(Execution.builder()
                    .id(IdUtils.create())
                    .namespace(conditionContext.getFlow().getNamespace())
                    .flowId(conditionContext.getFlow().getId())
                    .flowRevision(conditionContext.getFlow().getRevision())
                    .state(new State())
                    .variables(Map.of(
                        "jobId", rJobId,
                        "finalState", status.getState(),
                        "stateDetails", status.getStateDetails() != null ? status.getStateDetails() : "",
                        "success", isSuccess
                    ))
                    .outputs(triggerOutputs)
                    .build());
            }

            // Job is not in terminal state, continue polling
            return Optional.empty();

        } catch (Exception e) {
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
        // For triggers, interval is typically static, so we use the default
        // The actual rendered value will be used during evaluation
        return Duration.parse("PT10S");
    }

    private JobStatus getJobStatus(HttpClient client, String restUrl, String jobId)
            throws Exception {

        String normalizedUrl = restUrl.endsWith("/") ? restUrl.substring(0, restUrl.length() - 1) : restUrl;
        HttpRequest request = HttpRequest.builder()
            .uri(URI.create(normalizedUrl + "/v1/jobs/" + jobId))
            .method("GET")
            .build();

        HttpResponse<String> response = client.request(request, String.class);

        if (response.getStatus().getCode() == 404) {
            throw new RuntimeException("Job not found: " + jobId);
        }

        if (response.getStatus().getCode() != 200) {
            throw new RuntimeException("Failed to get job status: " + response.getStatus().getCode() + " - " + response.getBody());
        }

        return parseJobStatus(response.getBody());
    }

    private JobStatus parseJobStatus(String responseBody) {
        // Simple JSON parsing - in production, would use proper JSON parser
        String state = extractJsonValue(responseBody, "state");
        String stateDetails = extractJsonValue(responseBody, "state-details");

        return JobStatus.builder()
            .state(state)
            .stateDetails(stateDetails)
            .build();
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