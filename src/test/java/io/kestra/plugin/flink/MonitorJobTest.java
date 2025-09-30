package io.kestra.plugin.flink;

import io.kestra.core.models.property.Property;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class MonitorJobTest {

    @Test
    void testMonitorJobTriggerCreation() {
        MonitorJob monitor = MonitorJob.builder()
            .id("test-monitor")
            .type(MonitorJob.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jobId(Property.of("test-job-id"))
            .failOnError(Property.of(false))
            .build();

        assertThat(monitor.getId(), is("test-monitor"));
        assertThat(monitor.getRestUrl(), notNullValue());
        assertThat(monitor.getJobId(), notNullValue());
        assertThat(monitor.getInterval(), notNullValue());
        assertThat(monitor.getFailOnError(), notNullValue());
    }

    @Test
    void testMonitorJobTriggerWithExpectedStates() {
        MonitorJob monitor = MonitorJob.builder()
            .id("test-monitor-states")
            .type(MonitorJob.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jobId(Property.of("test-job-id"))
            .expectedTerminalStates(Property.of(Arrays.asList("FINISHED", "CANCELED")))
            .build();

        assertThat(monitor.getExpectedTerminalStates(), notNullValue());
    }

    @Test
    void testMonitorJobTriggerDefaults() {
        MonitorJob monitor = MonitorJob.builder()
            .id("test-monitor-defaults")
            .type(MonitorJob.class.getName())
            .restUrl(Property.of("http://localhost:8081"))
            .jobId(Property.of("test-job-id"))
            .build();

        // Test that defaults are properly initialized for trigger
        assertThat(monitor.getInterval(), is(Duration.parse("PT10S")));
        assertThat(monitor.getFailOnError(), notNullValue());
    }
}