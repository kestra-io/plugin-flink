package io.kestra.plugin.flink;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.FlowInterface;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@KestraTest
@EnabledIfSystemProperty(named = "flink.integration.realCluster", matches = "true")
class FlinkRealClusterCompatibilityTest {
    private static final String REST_URL = System.getProperty("flink.integration.restUrl", "http://localhost:8081");
    private static final String FIXTURE_CLASS = "io.kestra.plugin.flink.fixture.CheckpointedStreamingJob";
    private static final String SAVEPOINT_DIRECTORY = "file:///tmp";
    private static final String EXPECTED_FLINK_VERSION = System.getProperty("flink.integration.expectedVersion", "1.20.1");
    private static final String COMPOSE_FILE = System.getProperty("flink.integration.composeFile", "docker-compose-ci.yml");
    private static final Duration JOB_START_TIMEOUT = Duration.ofSeconds(60);
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final ObjectMapper JSON = new ObjectMapper();

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void verifyClusterAndFixture() throws Exception {
        var overview = getJson(REST_URL + "/overview");
        assertEquals(EXPECTED_FLINK_VERSION, overview.path("flink-version").asText());
        assertTrue(overview.path("taskmanagers").asInt() > 0, "A TaskManager must be registered");
        assertTrue(overview.path("slots-available").asInt() > 0, "At least one task slot must be available");

        var fixturePath = Path.of(System.getProperty("flink.integration.fixtureJar"));
        assertTrue(Files.isRegularFile(fixturePath), "The Java 17 fixture JAR must exist: " + fixturePath);
        try (var fixture = new JarFile(fixturePath.toFile())) {
            var entry = fixture.getJarEntry(FIXTURE_CLASS.replace('.', '/') + ".class");
            assertNotNull(entry, "The fixture entry class must be packaged");
            try (var bytecode = fixture.getInputStream(entry)) {
                var header = bytecode.readNBytes(8);
                assertEquals(8, header.length, "The fixture class file header must be readable");
                var majorVersion = (Byte.toUnsignedInt(header[6]) << 8) | Byte.toUnsignedInt(header[7]);
                assertEquals(61, majorVersion, "The fixture must use Java 17-compatible bytecode");
            }
        }
    }

    @Test
    void monitorJobReadsRunningAndTerminalStatesFromFlink() throws Exception {
        try (var job = submitRunningJob("monitor")) {
            assertEquals("RUNNING", waitForJobState(job.jobId(), "RUNNING", JOB_START_TIMEOUT));

            var monitor = monitorTask(job.jobId());
            assertTrue(
                evaluate(monitor).isEmpty(),
                "Monitoring a running job must not trigger a terminal-state execution"
            );

            cancelJob(job.jobId());
            assertEquals("CANCELED", waitForJobState(job.jobId(), "CANCELED", JOB_START_TIMEOUT));

            var terminal = evaluate(monitor).orElseThrow();
            assertEquals(job.jobId(), terminal.getOutputs().get("jobId"));
            assertEquals("CANCELED", terminal.getOutputs().get("finalState"));
            assertEquals(true, terminal.getOutputs().get("success"));
        }
    }

    @Test
    void triggerSavepointCompletesAndReturnsAnAccessibleLocation() throws Exception {
        try (var job = submitRunningJob("savepoint")) {
            assertEquals("RUNNING", waitForJobState(job.jobId(), "RUNNING", JOB_START_TIMEOUT));

            var task = TriggerSavepoint.builder()
                .id("real-cluster-savepoint")
                .type(TriggerSavepoint.class.getName())
                .restUrl(Property.ofValue(REST_URL))
                .jobId(Property.ofValue(job.jobId()))
                .targetDirectory(Property.ofValue(SAVEPOINT_DIRECTORY))
                .savepointTimeout(Property.ofValue(30))
                .build();

            var output = task.run(runContext(task));
            var location = URI.create(output.getSavepointPath());
            try (var savepoint = new ContainerPath(location.getPath())) {
                assertEquals(job.jobId(), output.getJobId());
                assertEquals("file", location.getScheme());
                assertTrue(
                    location.getPath().startsWith("/tmp/savepoint-"),
                    "Flink must return a location under the configured savepoint directory: " + location
                );
                assertContainerPathExists("jobmanager", location.getPath());
                assertContainerPathExists("jobmanager", location.getPath() + "/_metadata");
            }
        }
    }

    private RunningJob submitRunningJob(String id) throws Exception {
        var task = Submit.builder()
            .id("real-cluster-submit-" + id)
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(REST_URL))
            .jarUri(Property.ofValue(fixturePath().toUri().toString()))
            .entryClass(Property.ofValue(FIXTURE_CLASS))
            .parallelism(Property.ofValue(1))
            .build();
        var output = task.run(runContext(task));
        assertNotNull(output.getJobId(), "Flink must return a job ID");
        return new RunningJob(output.getJobId());
    }

    private Path fixturePath() {
        return Path.of(System.getProperty("flink.integration.fixtureJar"));
    }

    private MonitorJob monitorTask(String jobId) {
        return MonitorJob.builder()
            .id("real-cluster-monitor")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue(REST_URL))
            .jobId(Property.ofValue(jobId))
            .expectedTerminalStates(Property.ofValue(List.of("CANCELED")))
            .build();
    }

    private Optional<Execution> evaluate(MonitorJob task) throws Exception {
        var flow = mock(FlowInterface.class);
        when(flow.getNamespace()).thenReturn("company.team");
        when(flow.getId()).thenReturn("flink-compatibility-test");
        when(flow.getRevision()).thenReturn(1);
        var contextTask = Submit.builder()
            .id("real-cluster-monitor-context")
            .type(Submit.class.getName())
            .restUrl(Property.ofValue(REST_URL))
            .jarUri(Property.ofValue(fixturePath().toUri().toString()))
            .entryClass(Property.ofValue(FIXTURE_CLASS))
            .build();
        var conditionContext = new ConditionContext(flow, null, runContext(contextTask), Map.of(), null);
        return task.evaluate(conditionContext, new TriggerContext());
    }

    private void cancelJob(String jobId) throws Exception {
        var task = CancelJob.builder()
            .id("real-cluster-cleanup")
            .type(CancelJob.class.getName())
            .restUrl(Property.ofValue(REST_URL))
            .jobId(Property.ofValue(jobId))
            .build();
        task.run(runContext(task));
    }

    private RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(runContextFactory, task, Map.of());
    }

    private String waitForJobState(String jobId, String expectedState, Duration timeout) throws Exception {
        var deadline = Instant.now().plus(timeout);
        String state = "UNKNOWN";
        while (Instant.now().isBefore(deadline)) {
            var job = getJson(REST_URL + "/jobs/" + jobId);
            state = job.path("state").asText();
            if (expectedState.equals(state) && ("RUNNING".equals(state) ? allVerticesRunning(job) : true)) {
                return state;
            }
            if (isTerminal(state) && !expectedState.equals(state)) {
                fail("Flink job reached " + state + " while waiting for " + expectedState);
            }
            Thread.sleep(200);
        }
        fail("Timed out waiting for Flink job " + jobId + " to reach " + expectedState + "; last state was " + state);
        return state;
    }

    private boolean allVerticesRunning(JsonNode job) {
        var vertices = job.path("vertices");
        if (!vertices.isArray() || vertices.isEmpty()) {
            return false;
        }
        for (var vertex : vertices) {
            if (!"RUNNING".equals(vertex.path("status").asText())) {
                return false;
            }
        }
        return true;
    }

    private JsonNode getJson(String url) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build();
        var response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("GET " + url + " returned " + response.statusCode() + ": " + response.body());
        }
        return JSON.readTree(response.body());
    }

    private boolean isTerminal(String state) {
        return "FINISHED".equals(state) || "CANCELED".equals(state) || "FAILED".equals(state);
    }

    private void assertContainerPathExists(String service, String path) throws Exception {
        runComposeCommand("exec", "-T", service, "test", "-e", path);
    }

    private String runComposeCommand(String... arguments) throws Exception {
        var command = new ArrayList<String>();
        command.add("docker");
        command.add("compose");
        command.add("-f");
        command.add(COMPOSE_FILE);
        command.addAll(List.of(arguments));

        var process = new ProcessBuilder(command)
            .directory(Path.of(System.getProperty("user.dir")).toFile())
            .redirectErrorStream(true)
            .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IOException("Timed out running: " + String.join(" ", command));
        }
        var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (process.exitValue() != 0) {
            throw new IOException(
                "Command failed (" + process.exitValue() + "): " + String.join(" ", command) + System.lineSeparator() + output
            );
        }
        return output;
    }

    private final class RunningJob implements AutoCloseable {
        private final String jobId;

        private RunningJob(String jobId) {
            this.jobId = jobId;
        }

        private String jobId() {
            return jobId;
        }

        @Override
        public void close() throws Exception {
            var state = getJson(REST_URL + "/jobs/" + jobId).path("state").asText();
            if (!isTerminal(state)) {
                cancelJob(jobId);
                waitForJobState(jobId, "CANCELED", JOB_START_TIMEOUT);
            }
        }
    }

    private final class ContainerPath implements AutoCloseable {
        private final String path;

        private ContainerPath(String path) {
            this.path = path;
        }

        @Override
        public void close() throws Exception {
            Exception failure = null;
            for (var service : List.of("jobmanager", "taskmanager")) {
                try {
                    runComposeCommand("exec", "-T", service, "rm", "-rf", "--", path);
                } catch (Exception e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
