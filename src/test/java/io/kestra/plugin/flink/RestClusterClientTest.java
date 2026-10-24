package io.kestra.plugin.flink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.flows.FlowInterface;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@KestraTest
class RestClusterClientTest {
    private static final String JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final String JAR_ID = "uploaded-job-jar.jar";
    private static final String REQUEST_ID = "request-123";
    private WireMockServer server;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void startWireMock() {
        server = new WireMockServer(0);
        server.start();
    }

    @AfterEach
    void stopWireMock() {
        server.stop();
    }

    @Test
    void submitsJobAndPreservesJarAndJobOutputs() throws Exception {
        server.stubFor(
            post(urlEqualTo("/v1/jars/upload"))
                .willReturn(okJson("{\"filename\":\"/uploads/" + JAR_ID + "\",\"status\":\"success\"}"))
        );
        server.stubFor(
            post(urlEqualTo("/v1/jars/" + JAR_ID + "/run"))
                .withRequestBody(
                    equalToJson(
                        "{\"entryClass\":\"" + WorkerExecutionGuardJob.class.getName() + "\",\"parallelism\":1}"
                    )
                )
                .willReturn(okJson("{\"jobid\":\"" + JOB_ID + "\"}"))
        );
        var jar = createJobJar(WorkerExecutionGuardJob.class);
        var task = Submit.builder()
            .id("rest-client-submit")
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue(jar.toUri().toString()))
            .entryClass(Property.ofValue(WorkerExecutionGuardJob.class.getName()))
            .parallelism(Property.ofValue(1))
            .build();

        try {
            var output = task.run(runContext(task));

            assertThat(output.getJobId(), is(JOB_ID));
            assertThat(output.getJarId(), is(JAR_ID));
            server.verify(
                postRequestedFor(urlEqualTo("/v1/jars/upload"))
                    .withHeader("Content-Type", containing("multipart/form-data"))
            );
            server.verify(
                postRequestedFor(urlEqualTo("/v1/jars/" + JAR_ID + "/run"))
                    .withRequestBody(
                        equalToJson(
                            "{\"entryClass\":\"" + WorkerExecutionGuardJob.class.getName() + "\",\"parallelism\":1}"
                        )
                    )
            );
        } finally {
            Files.deleteIfExists(jar);
        }
    }

    @Test
    void submitsWithoutExplicitParallelismAndLeavesDefaultToCluster() throws Exception {
        server.stubFor(
            post(urlEqualTo("/v1/jars/upload"))
                .willReturn(okJson("{\"filename\":\"/uploads/" + JAR_ID + "\",\"status\":\"success\"}"))
        );
        server.stubFor(
            post(urlEqualTo("/v1/jars/" + JAR_ID + "/run"))
                .withRequestBody(equalToJson("{\"entryClass\":\"" + NoOpJob.class.getName() + "\"}"))
                .willReturn(okJson("{\"jobid\":\"" + JOB_ID + "\"}"))
        );
        var jar = createJobJar();
        var task = Submit.builder()
            .id("rest-client-submit-cluster-default")
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue(jar.toUri().toString()))
            .entryClass(Property.ofValue(NoOpJob.class.getName()))
            .build();

        try {
            var output = task.run(runContext(task));

            assertThat(output.getJobId(), is(JOB_ID));
            assertThat(output.getJarId(), is(JAR_ID));
            server.verify(
                postRequestedFor(urlEqualTo("/v1/jars/" + JAR_ID + "/run"))
                    .withRequestBody(equalToJson("{\"entryClass\":\"" + NoOpJob.class.getName() + "\"}"))
            );
        } finally {
            Files.deleteIfExists(jar);
        }
    }

    @Test
    void submitsRenderedArgumentsConfigurationAndSavepointSettings() throws Exception {
        server.stubFor(
            post(urlEqualTo("/v1/jars/upload"))
                .willReturn(okJson("{\"filename\":\"/uploads/" + JAR_ID + "\",\"status\":\"success\"}"))
        );
        var requestBody = "{\"entryClass\":\"" + NoOpJob.class.getName()
            + "\",\"parallelism\":3,\"programArgsList\":[\"--input\",\"s3://input data/\"],"
            + "\"savepointPath\":\"s3://savepoints/restore\",\"allowNonRestoredState\":true,"
            + "\"flinkConfiguration\":{\"execution.checkpointing.interval\":\"10s\"}}";
        server.stubFor(
            post(urlEqualTo("/v1/jars/" + JAR_ID + "/run"))
                .withRequestBody(equalToJson(requestBody))
                .willReturn(okJson("{\"jobid\":\"" + JOB_ID + "\"}"))
        );
        var jar = createJobJar();
        var task = Submit.builder()
            .id("rest-client-submit-options")
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue(jar.toUri().toString()))
            .entryClass(Property.ofValue(NoOpJob.class.getName()))
            .args(Property.ofValue(List.of("--input", "s3://input data/")))
            .parallelism(Property.ofValue(3))
            .restoreFromSavepoint(Property.ofValue("s3://savepoints/restore"))
            .allowNonRestoredState(Property.ofValue(true))
            .jobConfig(Property.ofValue(Map.of("execution.checkpointing.interval", "10s")))
            .build();

        try {
            var output = task.run(runContext(task));

            assertThat(output.getJobId(), is(JOB_ID));
            assertThat(output.getJarId(), is(JAR_ID));
            server.verify(
                postRequestedFor(urlEqualTo("/v1/jars/" + JAR_ID + "/run"))
                    .withRequestBody(equalToJson(requestBody))
            );
        } finally {
            Files.deleteIfExists(jar);
        }
    }

    @Test
    void cancelsJobThroughKestraHttpClient() throws Exception {
        server.stubFor(
            patch(urlEqualTo("/v1/jobs/" + JOB_ID))
                .willReturn(aResponse().withStatus(202))
        );
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID))
                .inScenario("job-cancellation")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(okJson("{\"state\":\"RUNNING\"}"))
                .willSetStateTo("job-cancelled")
        );
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID))
                .inScenario("job-cancellation")
                .whenScenarioStateIs("job-cancelled")
                .willReturn(okJson("{\"state\":\"CANCELED\"}"))
        );
        var task = CancelJob.builder()
            .id("rest-client-cancel")
            .type(CancelJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .build();

        var output = task.run(runContext(task));

        assertThat(output.getJobId(), is(JOB_ID));
        assertThat(output.getCancellationResult(), is("Job cancellation requested"));
        server.verify(patchRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID)));
        server.verify(getRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID)));
    }

    @Test
    void cancelsWithSingleSavepointRequestAndPreservesSavepointPath() throws Exception {
        server.stubFor(
            post(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints"))
                .withRequestBody(
                    equalToJson(
                        "{\"target-directory\":\"s3://savepoints/cancel\",\"cancel-job\":true,\"formatType\":\"CANONICAL\"}"
                    )
                )
                .willReturn(
                    aResponse()
                        .withStatus(202)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"request-id\":\"" + REQUEST_ID + "\"}")
                )
        );
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID))
                .willReturn(
                    okJson(
                        "{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"location\":\"s3://savepoints/cancel/complete\"}}"
                    )
                )
        );
        var task = CancelJob.builder()
            .id("rest-client-cancel-savepoint")
            .type(CancelJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .withSavepoint(Property.ofValue(true))
            .savepointDir(Property.ofValue("s3://savepoints/cancel"))
            .build();

        var output = task.run(runContext(task));

        assertThat(output.getJobId(), is(JOB_ID));
        assertThat(output.getSavepointPath(), is("s3://savepoints/cancel/complete"));
        server.verify(1, postRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints")));
        server.verify(0, patchRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID)));
        server.verify(1, getRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID)));
    }

    @Test
    void drainsJobWithOneStopSavepointRequest() throws Exception {
        server.stubFor(
            post(urlEqualTo("/v1/jobs/" + JOB_ID + "/stop"))
                .withRequestBody(equalToJson("{\"drain\":true,\"formatType\":\"CANONICAL\"}"))
                .willReturn(
                    aResponse()
                        .withStatus(202)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"request-id\":\"" + REQUEST_ID + "\"}")
                )
        );
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID))
                .willReturn(okJson("{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"location\":\"s3://default/drain\"}}"))
        );
        var task = CancelJob.builder()
            .id("rest-client-drain")
            .type(CancelJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .drainJob(Property.ofValue(true))
            .build();

        var output = task.run(runContext(task));

        assertThat(output.getJobId(), is(JOB_ID));
        assertThat(output.getSavepointPath(), nullValue());
        assertThat(output.getCancellationResult(), is("Job stop requested (drain mode)"));
        server.verify(1, postRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID + "/stop")));
        server.verify(0, postRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints")));
        server.verify(0, patchRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID)));
    }

    @Test
    void drainsWithConfiguredSavepointDirectoryWithoutTriggeringASecondSavepoint() throws Exception {
        server.stubFor(
            post(urlEqualTo("/v1/jobs/" + JOB_ID + "/stop"))
                .withRequestBody(
                    equalToJson(
                        "{\"targetDirectory\":\"s3://savepoints/drained\",\"drain\":true,\"formatType\":\"CANONICAL\"}"
                    )
                )
                .willReturn(
                    aResponse()
                        .withStatus(202)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"request-id\":\"" + REQUEST_ID + "\"}")
                )
        );
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID))
                .willReturn(
                    okJson(
                        "{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"location\":\"s3://savepoints/drained/complete\"}}"
                    )
                )
        );
        var task = CancelJob.builder()
            .id("rest-client-drain-savepoint")
            .type(CancelJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .withSavepoint(Property.ofValue(true))
            .savepointDir(Property.ofValue("s3://savepoints/drained"))
            .drainJob(Property.ofValue(true))
            .build();

        var output = task.run(runContext(task));

        assertThat(output.getSavepointPath(), is("s3://savepoints/drained/complete"));
        server.verify(1, postRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID + "/stop")));
        server.verify(0, postRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints")));
    }

    @Test
    void requiresSavepointDirectoryWhenSavepointIsEnabled() throws Exception {
        var task = CancelJob.builder()
            .id("cancel-without-savepoint-directory")
            .type(CancelJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .withSavepoint(Property.ofValue(true))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext(task)));
        server.verify(0, anyRequestedFor(anyUrl()));
    }

    @Test
    void failsWhenCancellationDoesNotCompleteWithinTheConfiguredTimeout() throws Exception {
        server.stubFor(
            patch(urlEqualTo("/v1/jobs/" + JOB_ID))
                .willReturn(aResponse().withStatus(202))
        );
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID))
                .willReturn(okJson("{\"state\":\"RUNNING\"}"))
        );
        var task = CancelJob.builder()
            .id("cancel-timeout")
            .type(CancelJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .cancellationTimeout(Property.ofValue(1))
            .build();

        assertThrows(TimeoutException.class, () -> task.run(runContext(task)));
    }

    @Test
    void monitorsTerminalJobThroughRestClusterClientAndKeepsStateDetails() throws Exception {
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID))
                .willReturn(okJson("{\"state\":\"FINISHED\",\"state-details\":\"completed with warnings\"}"))
        );
        var task = MonitorJob.builder()
            .id("rest-client-monitor")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .build();
        var runContextTask = Submit.builder()
            .id("rest-client-monitor-context")
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue("file:///tmp/monitor-context.jar"))
            .entryClass(Property.ofValue("com.example.Main"))
            .build();
        var runContext = runContext(runContextTask);
        var flow = Mockito.mock(FlowInterface.class);
        when(flow.getNamespace()).thenReturn("company.team");
        when(flow.getId()).thenReturn("monitored-flow");
        when(flow.getRevision()).thenReturn(1);
        var conditionContext = new ConditionContext(flow, null, runContext, Map.of(), null);

        try (
            var clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> when(client.getJobStatus(Mockito.any(JobID.class)))
                    .thenReturn(CompletableFuture.completedFuture(JobStatus.FINISHED))
            )
        ) {
            var execution = task.evaluate(conditionContext, new TriggerContext());

            assertThat(execution.isPresent(), is(true));
            assertThat(execution.orElseThrow().getOutputs().get("jobId"), is(JOB_ID));
            assertThat(execution.orElseThrow().getOutputs().get("finalState"), is("FINISHED"));
            assertThat(execution.orElseThrow().getOutputs().get("stateDetails"), is("completed with warnings"));
            Mockito.verify(clients.constructed().getFirst()).getJobStatus(Mockito.eq(JobID.fromHexString(JOB_ID)));
            server.verify(getRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID)));
        }
    }

    @Test
    void rejectsInvalidJobIdBeforeCreatingRestClient() throws Exception {
        var task = MonitorJob.builder()
            .id("monitor-invalid-job-id")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue("not-a-job-id"))
            .build();

        assertThrows(
            IllegalArgumentException.class, () -> task.evaluate(
                monitorConditionContext(task),
                new TriggerContext()
            )
        );
    }

    @Test
    void treatsTransientMonitorFailureAsAnEmptyPoll() throws Exception {
        var task = MonitorJob.builder()
            .id("monitor-transient-failure")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .build();

        try (
            var clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> when(client.getJobStatus(Mockito.any(JobID.class)))
                    .thenReturn(CompletableFuture.failedFuture(new IOException("temporary connection failure")))
            )
        ) {
            var execution = task.evaluate(
                monitorConditionContext(task),
                new TriggerContext()
            );

            assertThat(execution.isEmpty(), is(true));
        }
    }

    @Test
    void propagatesPermanentMonitorFailure() throws Exception {
        var task = MonitorJob.builder()
            .id("monitor-permanent-failure")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .build();

        try (
            var clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> when(client.getJobStatus(Mockito.any(JobID.class)))
                    .thenReturn(CompletableFuture.failedFuture(new IllegalArgumentException("invalid request")))
            )
        ) {
            assertThrows(
                IllegalArgumentException.class,
                () -> task.evaluate(monitorConditionContext(task), new TriggerContext())
            );
        }
    }

    @Test
    void completesTerminalMonitorPollWhenStateDetailsRequestFails() throws Exception {
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID))
                .willReturn(aResponse().withStatus(500).withBody("temporary failure"))
        );
        var task = MonitorJob.builder()
            .id("monitor-state-details-failure")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .build();

        try (
            var clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> when(client.getJobStatus(Mockito.any(JobID.class)))
                    .thenReturn(CompletableFuture.completedFuture(JobStatus.FINISHED))
            )
        ) {
            var execution = task.evaluate(
                monitorConditionContext(task),
                new TriggerContext()
            );

            assertThat(execution.isPresent(), is(true));
            assertThat(execution.orElseThrow().getOutputs().get("finalState"), is("FINISHED"));
            assertThat(execution.orElseThrow().getOutputs().get("stateDetails"), is(""));
        }
    }

    @Test
    void monitorsFailedJobAsConfiguredTerminalState() throws Exception {
        assertConfiguredTerminalState(JobStatus.FAILED, "FAILED", "operator failed");
    }

    @Test
    void monitorsCanceledJobAsConfiguredTerminalState() throws Exception {
        assertConfiguredTerminalState(JobStatus.CANCELED, "CANCELED", "job canceled");
    }

    private void assertConfiguredTerminalState(JobStatus jobStatus, String expectedState, String details) throws Exception {
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID))
                .willReturn(okJson("{\"state\":\"" + expectedState + "\",\"state-details\":\"" + details + "\"}"))
        );
        var task = MonitorJob.builder()
            .id("rest-client-monitor-" + expectedState.toLowerCase())
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .expectedTerminalStates(Property.ofValue(List.of(expectedState)))
            .build();
        var runContextTask = Submit.builder()
            .id("rest-client-monitor-context-" + expectedState.toLowerCase())
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue("file:///tmp/monitor-context.jar"))
            .entryClass(Property.ofValue("com.example.Main"))
            .build();
        var flow = Mockito.mock(FlowInterface.class);
        when(flow.getNamespace()).thenReturn("company.team");
        when(flow.getId()).thenReturn("monitored-flow");
        when(flow.getRevision()).thenReturn(1);
        var conditionContext = new ConditionContext(flow, null, runContext(runContextTask), Map.of(), null);

        try (
            var clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> when(client.getJobStatus(Mockito.any(JobID.class)))
                    .thenReturn(CompletableFuture.completedFuture(jobStatus))
            )
        ) {
            var execution = task.evaluate(conditionContext, new TriggerContext());

            assertThat(execution.isPresent(), is(true));
            assertThat(execution.orElseThrow().getOutputs().get("jobId"), is(JOB_ID));
            assertThat(execution.orElseThrow().getOutputs().get("finalState"), is(expectedState));
            assertThat(execution.orElseThrow().getOutputs().get("stateDetails"), is(details));
            assertThat(execution.orElseThrow().getOutputs().get("success"), is(true));
            Mockito.verify(clients.constructed().getFirst()).getJobStatus(Mockito.eq(JobID.fromHexString(JOB_ID)));
            server.verify(getRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID)));
        }
    }

    private RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(runContextFactory, task, new HashMap<>());
    }

    private ConditionContext monitorConditionContext(MonitorJob task) {
        var runContextTask = Submit.builder()
            .id("monitor-context-" + task.getId())
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue("file:///tmp/monitor-context.jar"))
            .entryClass(Property.ofValue("com.example.Main"))
            .build();
        var flow = Mockito.mock(FlowInterface.class);
        when(flow.getNamespace()).thenReturn("company.team");
        when(flow.getId()).thenReturn("monitored-flow");
        when(flow.getRevision()).thenReturn(1);
        return new ConditionContext(flow, null, runContext(runContextTask), Map.of(), null);
    }

    private Path createJobJar() throws IOException {
        return createJobJar(NoOpJob.class);
    }

    private Path createJobJar(Class<?> jobClass) throws IOException {
        var jar = Files.createTempFile("flink-no-op-job", ".jar");
        var resourceName = "/" + jobClass.getName().replace('.', '/') + ".class";
        try (
            var bytecode = jobClass.getResourceAsStream(resourceName);
            var output = new JarOutputStream(Files.newOutputStream(jar))
        ) {
            if (bytecode == null) {
                throw new IOException("Could not read test job bytecode: " + resourceName);
            }
            output.putNextEntry(new JarEntry(jobClass.getName().replace('.', '/') + ".class"));
            bytecode.transferTo(output);
            output.closeEntry();
        }
        return jar;
    }

    public static class NoOpJob {
        public static void main(String[] args) throws Exception {
            var environment = StreamExecutionEnvironment.getExecutionEnvironment();
            environment.fromData(1).print();
            environment.execute("test-no-op-job");
        }
    }

    public static class WorkerExecutionGuardJob {
        public static void main(String[] args) {
            throw new AssertionError("Flink job entry point must not run in the Kestra worker JVM");
        }
    }
}
