package io.kestra.plugin.flink;

import java.util.HashMap;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

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
            .restUrl(Property.ofValue("http://localhost:8081"))
            .jobId(Property.ofValue(jobId))
            .withSavepoint(Property.ofValue(true))
            .savepointDir(Property.ofValue("/tmp/flink-savepoints"))
            .drainJob(Property.ofValue(false))
            .cancellationTimeout(Property.ofValue(120))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, cancel, new HashMap<>());

        assertThat(cancel.getId(), is("test-cancel"));
        assertThat(cancel.getWithSavepoint(), is(Property.ofValue(true)));
        assertThat(cancel.getSavepointDir(), is(Property.ofValue("/tmp/flink-savepoints")));
        assertThat(cancel.getDrainJob(), is(Property.ofValue(false)));
        assertThat(cancel.getCancellationTimeout(), is(Property.ofValue(120)));

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
            .restUrl(Property.ofValue("http://localhost:8081"))
            .jobId(Property.ofValue(jobId))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, cancel, new HashMap<>());

        // Test that defaults are set
        assertThat(cancel.getWithSavepoint(), is(Property.ofValue(false)));
        assertThat(cancel.getDrainJob(), is(Property.ofValue(false)));
        assertThat(cancel.getCancellationTimeout(), is(Property.ofValue(60)));

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
            .restUrl(Property.ofValue("http://localhost:8081"))
            .jobId(Property.ofValue(jobId))
            .drainJob(Property.ofValue(true))
            .withSavepoint(Property.ofValue(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, cancel, new HashMap<>());

        assertThat(cancel.getId(), is("test-cancel-drain"));
        assertThat(cancel.getDrainJob(), is(Property.ofValue(true)));
        assertThat(cancel.getWithSavepoint(), is(Property.ofValue(false)));

        // Actually run the task
        CancelJob.Output output = cancel.run(runContext);
        assertThat(output.getJobId(), is(jobId));
    }
}