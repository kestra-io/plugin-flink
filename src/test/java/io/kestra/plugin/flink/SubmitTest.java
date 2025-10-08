package io.kestra.plugin.flink;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class SubmitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @EnabledIfSystemProperty(named = "flink.integration.test", matches = "true")
    void testSubmitTaskCreation() throws Exception {
        String jarUri = System.getProperty("flink.integration.jarUri");
        Assumptions.assumeTrue(jarUri != null && !jarUri.isBlank(), "Provide -Dflink.integration.jarUri with a valid Flink job JAR");
        Path jarPath = Path.of(URI.create(jarUri));
        Assumptions.assumeTrue(Files.exists(jarPath), () -> "JAR not found at " + jarUri);

        Submit submit = Submit.builder()
            .id("test-submit")
            .type(Submit.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jarUri(Property.of(jarUri))
            .entryClass(Property.of("com.example.Main"))
            .args(Property.of(Arrays.asList("--input", "test")))
            .parallelism(Property.of(4))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, submit, new HashMap<>());

        assertThat(submit.getId(), is("test-submit"));

        // Test property rendering with runContext
        String rUrl = runContext.render(submit.getRestUrl()).as(String.class).orElseThrow();
        String rJarUri = runContext.render(submit.getJarUri()).as(String.class).orElseThrow();
        String rEntryClass = runContext.render(submit.getEntryClass()).as(String.class).orElseThrow();
        Integer rParallelism = runContext.render(submit.getParallelism()).as(Integer.class).orElseThrow();

        assertThat(rUrl, is("http://localhost:8081"));
        assertThat(rJarUri, startsWith("file://"));
        assertThat(rEntryClass, is("com.example.Main"));
        assertThat(rParallelism, is(4));

        // Actually run the task
        Submit.Output output = submit.run(runContext);
        assertThat(output.getJobId(), notNullValue());
        assertThat(output.getJarId(), notNullValue());
    }

    @Test
    @EnabledIfSystemProperty(named = "flink.integration.test", matches = "true")
    void testSubmitTaskExecution() throws Exception {
        // This test requires a running Flink cluster
        String jarUri = System.getProperty("flink.integration.jarUri");
        Assumptions.assumeTrue(jarUri != null && !jarUri.isBlank(), "Provide -Dflink.integration.jarUri with a valid Flink job JAR");
        Path jarPath = Path.of(URI.create(jarUri));
        Assumptions.assumeTrue(Files.exists(jarPath), () -> "JAR not found at " + jarUri);

        Submit submit = Submit.builder()
            .id("test-submit")
            .type(Submit.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jarUri(Property.of(jarUri))
            .entryClass(Property.of("com.example.TestJob"))
            .args(Property.of(Arrays.asList("--test-mode")))
            .parallelism(Property.of(1))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, submit, new HashMap<>());
        Submit.Output output = submit.run(runContext);

        assertThat(output.getJobId(), notNullValue());
        assertThat(output.getJarId(), notNullValue());
    }

    @Test
    @EnabledIfSystemProperty(named = "flink.integration.test", matches = "true")
    void testSubmitTaskWithSavepoint() throws Exception {
        String jarUri = System.getProperty("flink.integration.jarUri");
        Assumptions.assumeTrue(jarUri != null && !jarUri.isBlank(), "Provide -Dflink.integration.jarUri with a valid Flink job JAR");
        Path jarPath = Path.of(URI.create(jarUri));
        Assumptions.assumeTrue(Files.exists(jarPath), () -> "JAR not found at " + jarUri);

        String savepointPath = System.getProperty("flink.integration.savepointPath");
        Assumptions.assumeTrue(savepointPath != null && !savepointPath.isBlank(), "Provide -Dflink.integration.savepointPath");

        Submit submit = Submit.builder()
            .id("test-submit-savepoint")
            .type(Submit.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jarUri(Property.of(jarUri))
            .entryClass(Property.of("com.example.Main"))
            .restoreFromSavepoint(Property.of(savepointPath))
            .allowNonRestoredState(Property.of(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, submit, new HashMap<>());

        // Test property rendering with runContext
        String rUrl = runContext.render(submit.getRestUrl()).as(String.class).orElseThrow();
        String rJarUri = runContext.render(submit.getJarUri()).as(String.class).orElseThrow();
        String rEntryClass = runContext.render(submit.getEntryClass()).as(String.class).orElseThrow();
        String rSavepoint = runContext.render(submit.getRestoreFromSavepoint()).as(String.class).orElseThrow();
        Boolean rAllowNonRestored = runContext.render(submit.getAllowNonRestoredState()).as(Boolean.class).orElseThrow();

        assertThat(rUrl, is("http://localhost:8081"));
        assertThat(rJarUri, startsWith("file://"));
        assertThat(rEntryClass, is("com.example.Main"));
        assertThat(rSavepoint, is(savepointPath));
        assertThat(rAllowNonRestored, is(true));

        // Actually run the task
        Submit.Output output = submit.run(runContext);
        assertThat(output.getJobId(), notNullValue());
        assertThat(output.getJarId(), notNullValue());
    }

    @Test
    @EnabledIfSystemProperty(named = "flink.integration.test", matches = "true")
    void testSubmitTaskWithJobConfig() throws Exception {
        String jarUri = System.getProperty("flink.integration.jarUri");
        Assumptions.assumeTrue(jarUri != null && !jarUri.isBlank(), "Provide -Dflink.integration.jarUri with a valid Flink job JAR");
        Path jarPath = Path.of(URI.create(jarUri));
        Assumptions.assumeTrue(Files.exists(jarPath), () -> "JAR not found at " + jarUri);

        Map<String, String> config = new HashMap<>();
        config.put("execution.checkpointing.interval", "30s");
        config.put("state.backend", "rocksdb");

        Submit submit = Submit.builder()
            .id("test-submit-config")
            .type(Submit.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jarUri(Property.of(jarUri))
            .entryClass(Property.of("com.example.Main"))
            .jobConfig(Property.of(config))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, submit, new HashMap<>());

        // Test property rendering with runContext
        @SuppressWarnings("unchecked")
        Map<String, String> rConfig = runContext.render(submit.getJobConfig()).as((Class<Map<String, String>>) (Class<?>) Map.class).orElseThrow();

        assertThat(rConfig, notNullValue());
        assertThat(rConfig.get("execution.checkpointing.interval"), is("30s"));
        assertThat(rConfig.get("state.backend"), is("rocksdb"));

        // Actually run the task
        Submit.Output output = submit.run(runContext);
        assertThat(output.getJobId(), notNullValue());
        assertThat(output.getJarId(), notNullValue());
    }
}