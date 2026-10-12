package io.kestra.plugin.flink;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.messages.ConfigurationInfo;
import org.apache.flink.runtime.rest.messages.ConfigurationInfoEntry;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.github.tomakehurst.wiremock.WireMockServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.flows.FlowInterface;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.when;

@KestraTest
class RestClusterClientTest {
    private static final String JOB_ID = "0123456789abcdef0123456789abcdef";
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
                .willReturn(okJson("{\"filename\":\"/uploads/job.jar\",\"status\":\"success\"}"))
        );
        Path jar = createJobJar();
        Submit task = Submit.builder()
            .id("rest-client-submit")
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue(jar.toUri().toString()))
            .entryClass(Property.ofValue(NoOpJob.class.getName()))
            .parallelism(Property.ofValue(1))
            .build();

        try (
            MockedConstruction<RestClusterClient> clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> when(client.submitJob(Mockito.any(ExecutionPlan.class)))
                    .thenReturn(CompletableFuture.completedFuture(JobID.fromHexString(JOB_ID)))
            )
        ) {
            Submit.Output output = task.run(runContext(task));

            assertThat(output.getJobId(), is(JOB_ID));
            assertThat(output.getJarId(), is("job.jar"));
            Mockito.verify(clients.constructed().get(0)).submitJob(Mockito.any(ExecutionPlan.class));
            server.verify(
                postRequestedFor(urlEqualTo("/v1/jars/upload"))
                    .withHeader("Content-Type", containing("multipart/form-data"))
            );
        } finally {
            Files.deleteIfExists(jar);
        }
    }

    @Test
    void submitsUsingClusterDefaultParallelismFromRestClusterClient() throws Exception {
        server.stubFor(
            post(urlEqualTo("/v1/jars/upload"))
                .willReturn(okJson("{\"filename\":\"/uploads/job.jar\",\"status\":\"success\"}"))
        );
        Path jar = createJobJar();
        Submit task = Submit.builder()
            .id("rest-client-submit-cluster-default")
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue(jar.toUri().toString()))
            .entryClass(Property.ofValue(NoOpJob.class.getName()))
            .build();
        ConfigurationInfo configuration = new ConfigurationInfo();
        configuration.add(new ConfigurationInfoEntry("parallelism.default", "5"));

        try (
            MockedConstruction<RestClusterClient> clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) ->
                {
                    Mockito.doReturn(CompletableFuture.completedFuture(configuration))
                        .when(client)
                        .sendRequest(Mockito.any(), Mockito.any(), Mockito.any());
                    when(client.submitJob(Mockito.any(ExecutionPlan.class)))
                        .thenReturn(CompletableFuture.completedFuture(JobID.fromHexString(JOB_ID)));
                }
            )
        ) {
            Submit.Output output = task.run(runContext(task));

            assertThat(output.getJobId(), is(JOB_ID));
            assertThat(output.getJarId(), is("job.jar"));
            assertThat(clients.constructed(), hasSize(2));
            Mockito.verify(clients.constructed().get(0)).sendRequest(Mockito.any(), Mockito.any(), Mockito.any());
            Mockito.verify(clients.constructed().get(1)).submitJob(Mockito.any(ExecutionPlan.class));
        } finally {
            Files.deleteIfExists(jar);
        }
    }

    @Test
    void cancelsJobThroughRestClusterClient() throws Exception {
        CancelJob task = CancelJob.builder()
            .id("rest-client-cancel")
            .type(CancelJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .build();

        try (
            MockedConstruction<RestClusterClient> clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) ->
                {
                    when(client.cancel(Mockito.any(JobID.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
                    when(client.getJobStatus(Mockito.any(JobID.class)))
                        .thenReturn(CompletableFuture.completedFuture(JobStatus.CANCELED));
                }
            )
        ) {
            CancelJob.Output output = task.run(runContext(task));

            assertThat(output.getJobId(), is(JOB_ID));
            assertThat(output.getCancellationResult(), is("Job cancellation requested"));
            Mockito.verify(clients.constructed().get(0)).cancel(Mockito.eq(JobID.fromHexString(JOB_ID)));
            Mockito.verify(clients.constructed().get(0)).getJobStatus(Mockito.eq(JobID.fromHexString(JOB_ID)));
        }
    }

    @Test
    void cancelsWithSavepointThroughRestClusterClient() throws Exception {
        CancelJob task = CancelJob.builder()
            .id("rest-client-cancel-savepoint")
            .type(CancelJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .withSavepoint(Property.ofValue(true))
            .savepointDir(Property.ofValue("s3://savepoints/cancel"))
            .build();

        try (
            MockedConstruction<RestClusterClient> clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) ->
                {
                    when(
                        client.cancelWithSavepoint(
                            Mockito.eq(JobID.fromHexString(JOB_ID)),
                            Mockito.eq("s3://savepoints/cancel"),
                            Mockito.any()
                        )
                    ).thenReturn(CompletableFuture.completedFuture("s3://savepoints/cancel/complete"));
                    when(client.getJobStatus(Mockito.any(JobID.class)))
                        .thenReturn(CompletableFuture.completedFuture(JobStatus.CANCELED));
                }
            )
        ) {
            CancelJob.Output output = task.run(runContext(task));

            assertThat(output.getSavepointPath(), is("s3://savepoints/cancel/complete"));
            Mockito.verify(clients.constructed().get(0)).cancelWithSavepoint(
                Mockito.eq(JobID.fromHexString(JOB_ID)),
                Mockito.eq("s3://savepoints/cancel"),
                Mockito.any()
            );
            Mockito.verify(clients.constructed().get(0), Mockito.never()).cancel(Mockito.any(JobID.class));
        }
    }

    @Test
    void drainsJobThroughRestClusterClient() throws Exception {
        CancelJob task = CancelJob.builder()
            .id("rest-client-drain")
            .type(CancelJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .drainJob(Property.ofValue(true))
            .build();

        try (
            MockedConstruction<RestClusterClient> clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> when(
                    client.stopWithSavepoint(
                        Mockito.eq(JobID.fromHexString(JOB_ID)),
                        Mockito.eq(true),
                        Mockito.isNull(),
                        Mockito.any()
                    )
                ).thenReturn(CompletableFuture.completedFuture("s3://savepoints/drained"))
            )
        ) {
            CancelJob.Output output = task.run(runContext(task));

            assertThat(output.getJobId(), is(JOB_ID));
            assertThat(output.getSavepointPath(), nullValue());
            assertThat(output.getCancellationResult(), is("Job stop requested (drain mode)"));
            Mockito.verify(clients.constructed().get(0)).stopWithSavepoint(
                Mockito.eq(JobID.fromHexString(JOB_ID)),
                Mockito.eq(true),
                Mockito.isNull(),
                Mockito.any()
            );
            Mockito.verify(clients.constructed().get(0), Mockito.never()).cancel(Mockito.any(JobID.class));
        }
    }

    @Test
    void monitorsTerminalJobThroughRestClusterClientAndKeepsStateDetails() throws Exception {
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID))
                .willReturn(okJson("{\"state\":\"FINISHED\",\"state-details\":\"completed with warnings\"}"))
        );
        MonitorJob task = MonitorJob.builder()
            .id("rest-client-monitor")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .build();
        Submit runContextTask = Submit.builder()
            .id("rest-client-monitor-context")
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue("file:///tmp/monitor-context.jar"))
            .entryClass(Property.ofValue("com.example.Main"))
            .build();
        RunContext runContext = runContext(runContextTask);
        FlowInterface flow = Mockito.mock(FlowInterface.class);
        when(flow.getNamespace()).thenReturn("company.team");
        when(flow.getId()).thenReturn("monitored-flow");
        when(flow.getRevision()).thenReturn(1);
        ConditionContext conditionContext = new ConditionContext(flow, null, runContext, Map.of(), null);

        try (
            MockedConstruction<RestClusterClient> clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> when(client.getJobStatus(Mockito.any(JobID.class)))
                    .thenReturn(CompletableFuture.completedFuture(JobStatus.FINISHED))
            )
        ) {
            Optional<io.kestra.core.models.executions.Execution> execution = task.evaluate(conditionContext, new TriggerContext());

            assertThat(execution.isPresent(), is(true));
            assertThat(execution.orElseThrow().getOutputs().get("jobId"), is(JOB_ID));
            assertThat(execution.orElseThrow().getOutputs().get("finalState"), is("FINISHED"));
            assertThat(execution.orElseThrow().getOutputs().get("stateDetails"), is("completed with warnings"));
            Mockito.verify(clients.constructed().get(0)).getJobStatus(Mockito.eq(JobID.fromHexString(JOB_ID)));
            server.verify(getRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID)));
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
        MonitorJob task = MonitorJob.builder()
            .id("rest-client-monitor-" + expectedState.toLowerCase())
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .expectedTerminalStates(Property.ofValue(java.util.List.of(expectedState)))
            .build();
        Submit runContextTask = Submit.builder()
            .id("rest-client-monitor-context-" + expectedState.toLowerCase())
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue("file:///tmp/monitor-context.jar"))
            .entryClass(Property.ofValue("com.example.Main"))
            .build();
        FlowInterface flow = Mockito.mock(FlowInterface.class);
        when(flow.getNamespace()).thenReturn("company.team");
        when(flow.getId()).thenReturn("monitored-flow");
        when(flow.getRevision()).thenReturn(1);
        ConditionContext conditionContext = new ConditionContext(flow, null, runContext(runContextTask), Map.of(), null);

        try (
            MockedConstruction<RestClusterClient> clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> when(client.getJobStatus(Mockito.any(JobID.class)))
                    .thenReturn(CompletableFuture.completedFuture(jobStatus))
            )
        ) {
            Optional<io.kestra.core.models.executions.Execution> execution = task.evaluate(conditionContext, new TriggerContext());

            assertThat(execution.isPresent(), is(true));
            assertThat(execution.orElseThrow().getOutputs().get("jobId"), is(JOB_ID));
            assertThat(execution.orElseThrow().getOutputs().get("finalState"), is(expectedState));
            assertThat(execution.orElseThrow().getOutputs().get("stateDetails"), is(details));
            assertThat(execution.orElseThrow().getOutputs().get("success"), is(true));
            Mockito.verify(clients.constructed().get(0)).getJobStatus(Mockito.eq(JobID.fromHexString(JOB_ID)));
            server.verify(getRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID)));
        }
    }

    private RunContext runContext(io.kestra.core.models.tasks.Task task) {
        return TestsUtils.mockRunContext(runContextFactory, task, new HashMap<>());
    }

    private Path createJobJar() throws IOException {
        Path jar = Files.createTempFile("flink-no-op-job", ".jar");
        String resourceName = "/" + NoOpJob.class.getName().replace('.', '/') + ".class";
        try (
            InputStream bytecode = NoOpJob.class.getResourceAsStream(resourceName);
            JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))
        ) {
            if (bytecode == null) {
                throw new IOException("Could not read test job bytecode: " + resourceName);
            }
            output.putNextEntry(new JarEntry(NoOpJob.class.getName().replace('.', '/') + ".class"));
            bytecode.transferTo(output);
            output.closeEntry();
        }
        return jar;
    }

    public static class NoOpJob {
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
            environment.fromData(1).print();
            environment.execute("test-no-op-job");
        }
    }
}
