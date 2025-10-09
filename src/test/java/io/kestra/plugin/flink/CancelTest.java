package io.kestra.plugin.flink;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class CancelJobTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testCancelJobTaskCreation() throws Exception {
        // First, we need a job ID - assume it's provided as a system property
        String jobId = System.getProperty("flink.integration.jobId");
        Assumptions.assumeTrue(jobId != null && !jobId.isBlank(), "Provide -Dflink.integration.jobId with a running job ID");

        CancelJob cancel = CancelJob.builder()
            .id("test-cancel")
            .type(CancelJob.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jobId(Property.of(jobId))
            .withSavepoint(Property.of(true))
            .savepointDir(Property.of("/tmp/flink-savepoints"))
            .drainJob(Property.of(false))
            .cancellationTimeout(Property.of(120))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, cancel, new HashMap<>());

        assertThat(cancel.getId(), is("test-cancel"));
        assertThat(cancel.getWithSavepoint(), is(Property.of(true)));
        assertThat(cancel.getSavepointDir(), is(Property.of("/tmp/flink-savepoints")));
        assertThat(cancel.getDrainJob(), is(Property.of(false)));
        assertThat(cancel.getCancellationTimeout(), is(Property.of(120)));

        // Actually run the task
        CancelJob.Output output = cancel.run(runContext);
        assertThat(output.getJobId(), is(jobId));
    }

    @Test
    void testCancelJobTaskDefaults() throws Exception {
        String jobId = System.getProperty("flink.integration.jobId");
        Assumptions.assumeTrue(jobId != null && !jobId.isBlank(), "Provide -Dflink.integration.jobId with a running job ID");

        CancelJob cancel = CancelJob.builder()
            .id("test-cancel-defaults")
            .type(CancelJob.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jobId(Property.of(jobId))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, cancel, new HashMap<>());

        // Test that defaults are set
        assertThat(cancel.getWithSavepoint(), is(Property.of(false)));
        assertThat(cancel.getDrainJob(), is(Property.of(false)));
        assertThat(cancel.getCancellationTimeout(), is(Property.of(60)));

        // Actually run the task
        CancelJob.Output output = cancel.run(runContext);
        assertThat(output.getJobId(), is(jobId));
    }

    @Test
    void testCancelJobTaskWithDrain() throws Exception {
        String jobId = System.getProperty("flink.integration.jobId");
        Assumptions.assumeTrue(jobId != null && !jobId.isBlank(), "Provide -Dflink.integration.jobId with a running job ID");

        CancelJob cancel = CancelJob.builder()
            .id("test-cancel-drain")
            .type(CancelJob.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jobId(Property.of(jobId))
            .drainJob(Property.of(true))
            .withSavepoint(Property.of(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, cancel, new HashMap<>());

        assertThat(cancel.getId(), is("test-cancel-drain"));
        assertThat(cancel.getDrainJob(), is(Property.of(true)));
        assertThat(cancel.getWithSavepoint(), is(Property.of(false)));

        // Actually run the task
        CancelJob.Output output = cancel.run(runContext);
        assertThat(output.getJobId(), is(jobId));
    }
}