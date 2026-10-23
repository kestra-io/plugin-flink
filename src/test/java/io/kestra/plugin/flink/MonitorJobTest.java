package io.kestra.plugin.flink;

import java.time.Duration;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class MonitorJobTest {

    @Test
    void testMonitorJobTriggerCreation() {
        MonitorJob monitor = MonitorJob.builder()
            .id("test-monitor")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue("http://localhost:8081"))
            .jobId(Property.ofValue("test-job-id"))
            .failOnError(Property.ofValue(false))
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
            .restUrl(Property.ofValue("http://localhost:8081"))
            .jobId(Property.ofValue("test-job-id"))
            .expectedTerminalStates(Property.ofValue(Arrays.asList("FINISHED", "CANCELED")))
            .build();

        assertThat(monitor.getExpectedTerminalStates(), notNullValue());
    }

    @Test
    void testMonitorJobTriggerDefaults() {
        MonitorJob monitor = MonitorJob.builder()
            .id("test-monitor-defaults")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue("http://localhost:8081"))
            .jobId(Property.ofValue("test-job-id"))
            .build();

        // Test that defaults are properly initialized for trigger
        assertThat(monitor.getInterval(), is(Duration.parse("PT10S")));
        assertThat(monitor.getFailOnError(), notNullValue());
    }

    @Test
    void testMonitorJobUsesConfiguredInterval() {
        var monitor = MonitorJob.builder()
            .id("test-monitor-custom-interval")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue("http://localhost:8081"))
            .jobId(Property.ofValue("test-job-id"))
            .interval(Property.ofValue(Duration.parse("PT30S")))
            .build();

        assertThat(monitor.getInterval(), is(Duration.parse("PT30S")));
    }

    @Test
    void testMonitorJobKeepsDefaultForTemplatedInterval() {
        var monitor = MonitorJob.builder()
            .id("test-monitor-templated-interval")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue("http://localhost:8081"))
            .jobId(Property.ofValue("test-job-id"))
            .interval(Property.ofExpression("{{ inputs.interval }}"))
            .build();

        assertThat(monitor.getInterval(), is(Duration.parse("PT10S")));
    }

    @Test
    void testMonitorJobKeepsDefaultForNullInterval() {
        var monitor = MonitorJob.builder()
            .id("test-monitor-null-interval")
            .type(MonitorJob.class.getName())
            .restUrl(Property.ofValue("http://localhost:8081"))
            .jobId(Property.ofValue("test-job-id"))
            .interval(null)
            .build();

        assertThat(monitor.getInterval(), is(Duration.parse("PT10S")));
    }
}