package io.kestra.plugin.flink;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

@KestraTest
class SubmitSqlTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

    @Test
    void httpConfigurationUsesConnectionTimeout() throws Exception {
        var task = task("http://localhost:8083").connectionTimeout(Property.ofValue(7)).build();

        assertThat(renderedConnectTimeout(task), is(Duration.ofSeconds(7)));
    }

    @Test
    void httpConfigurationDefaultsToThirtySeconds() throws Exception {
        var task = task("http://localhost:8083").build();

        assertThat(renderedConnectTimeout(task), is(Duration.ofSeconds(30)));
    }

    @Test
    void connectionTimeoutMustBeAtLeastOneSecond() {
        assertThat(modelValidator.isValid(task("http://localhost:8083").connectionTimeout(Property.ofValue(1)).build()).isEmpty(), is(true));
        assertThat(modelValidator.isValid(task("http://localhost:8083").connectionTimeout(Property.ofValue(0)).build()).isPresent(), is(true));
    }

    @Test
    void runExecutesStatementAndClosesTemporarySession() throws Exception {
        var requests = new CopyOnWriteArrayList<String>();
        var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> handleGateway(exchange, requests));
        server.start();

        try {
            var task = task("http://localhost:" + server.getAddress().getPort()).build();

            var output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

            assertThat(output.getStatus(), is("FINISHED"));
            assertThat(output.getSessionHandle(), is("session-1"));
            assertThat(output.getOperationHandle(), is("operation-1"));
            assertThat(
                requests,
                contains(
                    "POST /v1/sessions",
                    "POST /v1/sessions/session-1/statements",
                    "GET /v1/sessions/session-1/operations/operation-1/status",
                    "DELETE /v1/sessions/session-1"
                )
            );
        } finally {
            server.stop(0);
        }
    }

    private static void handleGateway(HttpExchange exchange, List<String> requests) throws IOException {
        var request = exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath();
        requests.add(request);

        var body = switch (request) {
            case "POST /v1/sessions" -> "{\"sessionHandle\":\"session-1\"}";
            case "POST /v1/sessions/session-1/statements" -> "{\"operationHandle\":\"operation-1\"}";
            case "GET /v1/sessions/session-1/operations/operation-1/status" -> "{\"status\":\"FINISHED\"}";
            default -> "{}";
        };

        var bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (var responseBody = exchange.getResponseBody()) {
            responseBody.write(bytes);
        }
    }

    private SubmitSql.SubmitSqlBuilder<?, ?> task(String gatewayUrl) {
        return SubmitSql.builder()
            .id(IdUtils.create())
            .type(SubmitSql.class.getName())
            .gatewayUrl(Property.ofValue(gatewayUrl))
            .statement(Property.ofValue("SELECT 1"));
    }

    private Duration renderedConnectTimeout(SubmitSql task) throws Exception {
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var configuration = task.httpConfiguration(runContext);

        return runContext.render(configuration.getTimeout().getConnectTimeout()).as(Duration.class).orElseThrow();
    }
}
