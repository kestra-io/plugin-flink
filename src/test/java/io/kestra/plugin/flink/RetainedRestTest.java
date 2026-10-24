package io.kestra.plugin.flink;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@KestraTest
class RetainedRestTest {
    private static final String JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final String REQUEST_ID = "abcdef0123456789abcdef0123456789";
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
    void readsTerminalJobStateDetails() throws Exception {
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID))
                .willReturn(okJson("{\"state\":\"FAILED\",\"state-details\":\"operator failed\"}"))
        );
        var task = MonitorJob.builder()
            .id("retained-rest-monitor")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .build();
        var contextTask = Submit.builder()
            .id("retained-rest-monitor-context")
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jarUri(Property.ofValue("file:///tmp/monitor-context.jar"))
            .entryClass(Property.ofValue("com.example.Main"))
            .build();

        assertThat(task.getStateDetails(runContext(contextTask), server.baseUrl() + "/", JOB_ID), is("operator failed"));
        server.verify(getRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID)));
    }

    @Test
    void preservesRequestIdWhilePollingUntilSavepointCompletes() throws Exception {
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID))
                .inScenario("savepoint-completion")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(okJson("{\"status\":{\"id\":\"IN_PROGRESS\"}}"))
                .willSetStateTo("completed")
        );
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID))
                .inScenario("savepoint-completion")
                .whenScenarioStateIs("completed")
                .willReturn(okJson("{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"location\":\"s3://savepoints/final\"}}"))
        );
        var task = savepointTask(false, 20);
        try (
            var clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> when(
                    client.triggerDetachedSavepoint(
                        eq(JobID.fromHexString(JOB_ID)),
                        isNull(),
                        eq(SavepointFormatType.CANONICAL)
                    )
                ).thenReturn(CompletableFuture.completedFuture(REQUEST_ID))
            )
        ) {
            var output = task.run(runContext(task));

            assertThat(output.getRequestId(), is(REQUEST_ID));
            assertThat(output.getSavepointPath(), is("s3://savepoints/final"));
            assertThat(output.getJobId(), is(JOB_ID));
            verify(clients.constructed().getFirst()).triggerDetachedSavepoint(
                eq(JobID.fromHexString(JOB_ID)),
                isNull(),
                eq(SavepointFormatType.CANONICAL)
            );
            server.verify(2, getRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID)));
        }
    }

    @Test
    void reportsSavepointFailure() throws Exception {
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID))
                .willReturn(
                    okJson(
                        "{\"status\":{\"id\":\"FAILED\"},\"operation\":{\"failure-cause\":{\"message\":\"checkpoint failed\"}}}"
                    )
                )
        );
        var task = savepointTask(true, 10);
        var response = new TriggerResponse(TriggerId.fromHexString(REQUEST_ID));
        try (
            var clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> doReturn(CompletableFuture.completedFuture(response))
                    .when(client)
                    .sendRequest(any(), any(), any())
            )
        ) {
            var exception = assertThrows(Exception.class, () -> task.run(runContext(task)));

            assertThat(exceptionChain(exception), containsString("Savepoint failed: checkpoint failed"));
            verify(clients.constructed().getFirst()).sendRequest(any(), any(), any());
            server.verify(getRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID)));
        }
    }

    @Test
    void sendsCancelFlagThroughRestClusterClientAndPreservesRequestId() throws Exception {
        server.stubFor(
            get(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID))
                .willReturn(okJson("{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"location\":\"s3://savepoints/canceled\"}}"))
        );
        var task = savepointTask(true, 10);
        var response = new TriggerResponse(TriggerId.fromHexString(REQUEST_ID));
        try (
            var clients = Mockito.mockConstruction(
                RestClusterClient.class,
                (client, context) -> doReturn(CompletableFuture.completedFuture(response))
                    .when(client)
                    .sendRequest(any(), any(), any())
            )
        ) {
            var output = task.run(runContext(task));

            assertThat(output.getRequestId(), is(REQUEST_ID));
            assertThat(output.getSavepointPath(), is("s3://savepoints/canceled"));
            var bodyCaptor = ArgumentCaptor.forClass(SavepointTriggerRequestBody.class);
            verify(clients.constructed().getFirst()).sendRequest(any(), any(), bodyCaptor.capture());
            assertThat(bodyCaptor.getValue().isCancelJob(), is(true));
            server.verify(getRequestedFor(urlEqualTo("/v1/jobs/" + JOB_ID + "/savepoints/" + REQUEST_ID)));
        }
    }

    private TriggerSavepoint savepointTask(boolean cancel, int timeoutSeconds) {
        return TriggerSavepoint.builder()
            .id("retained-rest-savepoint")
            .type(TriggerSavepoint.class.getName())
            .restUrl(Property.ofValue(server.baseUrl()))
            .jobId(Property.ofValue(JOB_ID))
            .cancelJob(Property.ofValue(cancel))
            .savepointTimeout(Property.ofValue(timeoutSeconds))
            .build();
    }

    private RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(runContextFactory, task, new HashMap<>());
    }

    private String exceptionChain(Throwable throwable) {
        var messages = new StringBuilder();
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            messages.append(current.getMessage()).append('\n');
        }
        return messages.toString();
    }
}
