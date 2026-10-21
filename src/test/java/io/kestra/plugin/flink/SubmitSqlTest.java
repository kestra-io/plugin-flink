package io.kestra.plugin.flink;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class SubmitSqlTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void httpConfigurationUsesConnectionTimeout() throws Exception {
        SubmitSql task = task().connectionTimeout(Property.ofValue(7)).build();

        assertThat(renderedConnectTimeout(task), is(Duration.ofSeconds(7)));
    }

    @Test
    void httpConfigurationDefaultsToThirtySeconds() throws Exception {
        SubmitSql task = task().build();

        assertThat(renderedConnectTimeout(task), is(Duration.ofSeconds(30)));
    }

    private SubmitSql.SubmitSqlBuilder<?, ?> task() {
        return SubmitSql.builder()
            .id("submit-sql")
            .type(SubmitSql.class.getName())
            .gatewayUrl(Property.ofValue("http://localhost:8083"))
            .statement(Property.ofValue("SELECT 1"));
    }

    private Duration renderedConnectTimeout(SubmitSql task) throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        HttpConfiguration configuration = task.httpConfiguration(runContext);

        return runContext.render(configuration.getTimeout().getConnectTimeout()).as(Duration.class).orElseThrow();
    }
}
