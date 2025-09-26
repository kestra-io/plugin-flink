package io.kestra.plugin.flink;

import io.kestra.core.models.property.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class CancelTest {

    @Test
    void testCancelTaskCreation() {
        Cancel cancel = Cancel.builder()
            .id("test-cancel")
            .type(Cancel.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jobId(Property.of("test-job-id"))
            .withSavepoint(Property.of(true))
            .savepointDir(Property.of("s3://savepoints/cancel"))
            .drainJob(Property.of(false))
            .cancellationTimeout(Property.of(120))
            .build();

        assertThat(cancel.getId(), is("test-cancel"));
        assertThat(cancel.getWithSavepoint(), is(Property.of(true)));
        assertThat(cancel.getSavepointDir(), is(Property.of("s3://savepoints/cancel")));
        assertThat(cancel.getDrainJob(), is(Property.of(false)));
        assertThat(cancel.getCancellationTimeout(), is(Property.of(120)));
    }

    @Test
    void testCancelTaskDefaults() {
        Cancel cancel = Cancel.builder()
            .id("test-cancel-defaults")
            .type(Cancel.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jobId(Property.of("test-job-id"))
            .build();

        // Test that defaults are set
        assertThat(cancel.getWithSavepoint(), is(Property.of(false)));
        assertThat(cancel.getDrainJob(), is(Property.of(false)));
        assertThat(cancel.getCancellationTimeout(), is(Property.of(60)));
    }

    @Test
    void testCancelTaskWithDrain() {
        Cancel cancel = Cancel.builder()
            .id("test-cancel-drain")
            .type(Cancel.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jobId(Property.of("test-job-id"))
            .drainJob(Property.of(true))
            .withSavepoint(Property.of(false))
            .build();

        assertThat(cancel.getId(), is("test-cancel-drain"));
        assertThat(cancel.getDrainJob(), is(Property.of(true)));
        assertThat(cancel.getWithSavepoint(), is(Property.of(false)));
    }
}