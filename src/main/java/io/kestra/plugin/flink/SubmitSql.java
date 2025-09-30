package io.kestra.plugin.flink;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import io.kestra.core.utils.RetryUtils;
import io.kestra.core.models.tasks.retrys.Exponential;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URI;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Submit a SQL statement to Flink SQL Gateway.",
    description = "This task submits a SQL statement to Apache Flink via the SQL Gateway. " +
                  "No JAR file is required as the SQL is executed directly by Flink."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a streaming SQL query",
            full = true,
            code = """
                id: flink-sql-streaming
                namespace: company.team

                tasks:
                  - id: run-sql
                    type: io.kestra.plugin.flink.SubmitSql
                    gatewayUrl: "http://flink-sql-gateway:8083"
                    statement: |
                      INSERT INTO enriched_orders
                      SELECT o.order_id, o.customer_id, c.name, o.amount, o.order_time
                      FROM orders o
                      JOIN customers c ON o.customer_id = c.id
                    sessionConfig:
                      catalog: "default_catalog"
                      database: "default_database"
                      configuration:
                        execution.runtime-mode: "streaming"
                        execution.checkpointing.interval: "30s"
                """
        ),
        @Example(
            title = "Execute a batch SQL query",
            full = true,
            code = """
                id: flink-sql-batch
                namespace: company.team

                tasks:
                  - id: run-batch-sql
                    type: io.kestra.plugin.flink.SubmitSql
                    gatewayUrl: "http://flink-sql-gateway:8083"
                    statement: |
                      CREATE TABLE daily_summary AS
                      SELECT DATE(order_time) as order_date,
                             COUNT(*) as order_count,
                             SUM(amount) as total_amount
                      FROM orders
                      WHERE order_time >= '2024-01-01'
                      GROUP BY DATE(order_time)
                    sessionConfig:
                      configuration:
                        execution.runtime-mode: "batch"
                """
        )
    }
)
public class SubmitSql extends Task implements RunnableTask<SubmitSql.Output> {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Schema(
        title = "SQL Gateway URL",
        description = "The base URL of the Flink SQL Gateway, e.g., 'http://flink-sql-gateway:8083'"
    )
    @NotNull
    private Property<String> gatewayUrl;

    @Schema(
        title = "SQL statement",
        description = "The SQL statement to execute. Supports both DDL and DML statements."
    )
    @NotNull
    private Property<String> statement;

    @Schema(
        title = "Session name",
        description = "Optional session name. If not provided, a random session will be created."
    )
    private Property<String> sessionName;

    @Schema(
        title = "Session configuration",
        description = "Session configuration including catalog, database, and Flink configuration properties."
    )
    private Property<SessionConfig> sessionConfig;

    @Schema(
        title = "Connection timeout",
        description = "Timeout for connecting to the SQL Gateway in seconds. Defaults to 30."
    )
    @Builder.Default
    private Property<Integer> connectionTimeout = Property.of(30);

    @Schema(
        title = "Statement timeout",
        description = "Timeout for SQL statement execution in seconds. Defaults to 300."
    )
    @Builder.Default
    private Property<Integer> statementTimeout = Property.of(300);

    @Schema(
        title = "Acceptable terminal states",
        description = "List of operation states to consider as successful completion. " +
                      "For streaming jobs, include 'RUNNING' - these sessions will be kept alive. " +
                      "For batch jobs, use ['FINISHED']. Defaults to ['FINISHED', 'RUNNING']."
    )
    private Property<java.util.List<String>> acceptableStates;

    @Override
    public SubmitSql.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rGatewayUrl = runContext.render(this.gatewayUrl).as(String.class).orElseThrow();
        String rStatement = runContext.render(this.statement).as(String.class).orElseThrow();

        logger.info("Executing SQL statement via Flink SQL Gateway at: {}", rGatewayUrl);

        // Create or get session
        String sessionHandle = createOrGetSession(runContext, rGatewayUrl);

        // Render sessionName before try block to avoid exception suppression in finally
        String sessionName = this.sessionName != null ?
            runContext.render(this.sessionName).as(String.class).orElse(null) : null;

        OperationResult result = null;
        boolean keepSessionOpen = false;
        try {
            // Execute SQL statement
            String operationHandle = executeStatement(runContext, rGatewayUrl, sessionHandle, rStatement);

            // Wait for completion and get results
            result = waitForOperationCompletion(runContext, rGatewayUrl, sessionHandle, operationHandle);
            keepSessionOpen = "RUNNING".equals(result.getStatus());

            logger.info("SQL statement executed successfully. Operation handle: {}", operationHandle);

            return Output.builder()
                .operationHandle(operationHandle)
                .sessionHandle(sessionHandle)
                .resultCount(result.getRowCount())
                .status(result.getStatus())
                .build();

        } finally {
            // Close session if we created it (no session name means we created a temporary session)
            // BUT keep it open if the operation is still running (streaming jobs)
            if (sessionName == null && !keepSessionOpen) {
                closeSession(runContext, rGatewayUrl, sessionHandle);
            }
        }
    }

    private String createOrGetSession(RunContext runContext, String gatewayUrl)
            throws Exception {

        HttpClient client = HttpClient.builder()
            .runContext(runContext)
            .build();

        String sessionName = this.sessionName != null ?
            runContext.render(this.sessionName).as(String.class).orElse(null) : null;

        if (sessionName != null) {
            // List existing sessions to find one with matching name
            HttpRequest listRequest = HttpRequest.builder()
                .uri(URI.create(gatewayUrl + "/v1/sessions"))
                .method("GET")
                .build();

            HttpResponse<String> listResponse = client.request(listRequest, String.class);
            if (listResponse.getStatus().getCode() == 200) {
                String existingSessionHandle = findSessionByName(listResponse.getBody(), sessionName);
                if (existingSessionHandle != null) {
                    runContext.logger().info("Using existing session: {} (handle: {})", sessionName, existingSessionHandle);
                    return existingSessionHandle;
                }
            }
        }

        // Create new session
        ObjectNode payload = JSON.createObjectNode();
        if (sessionName != null) {
            payload.put("sessionName", sessionName);
        }

        if (sessionConfig != null) {
            SessionConfig config = runContext.render(sessionConfig).as(SessionConfig.class).orElse(null);
            if (config != null) {
                if (config.getCatalog() != null) {
                    payload.put("catalog", config.getCatalog());
                }
                if (config.getDatabase() != null) {
                    payload.put("database", config.getDatabase());
                }
                if (config.getConfiguration() != null && !config.getConfiguration().isEmpty()) {
                    payload.set("properties", JSON.valueToTree(config.getConfiguration()));
                }
            }
        }

        HttpRequest request = HttpRequest.builder()
            .uri(URI.create(gatewayUrl + "/v1/sessions"))
            .method("POST")
            .addHeader("Content-Type", "application/json")
            .body(HttpRequest.StringRequestBody.builder()
                .content(JSON.writeValueAsString(payload))
                .build())
            .build();

        HttpResponse<String> response = client.request(request, String.class);

        if (response.getStatus().getCode() != 200) {
            throw new RuntimeException("Failed to create session: " + response.getStatus().getCode() + " - " + response.getBody());
        }

        // Extract session handle from response
        String sessionHandle = extractSessionHandleFromResponse(response.getBody());
        runContext.logger().info("Created new session: {}", sessionHandle);
        return sessionHandle;
    }

    private String executeStatement(RunContext runContext, String gatewayUrl, String sessionHandle, String statement)
            throws Exception {

        HttpClient client = HttpClient.builder()
            .runContext(runContext)
            .build();

        ObjectNode payload = JSON.createObjectNode().put("statement", statement);

        HttpRequest request = HttpRequest.builder()
            .uri(URI.create(gatewayUrl + "/v1/sessions/" + sessionHandle + "/statements"))
            .method("POST")
            .addHeader("Content-Type", "application/json")
            .body(HttpRequest.StringRequestBody.builder()
                .content(JSON.writeValueAsString(payload))
                .build())
            .build();

        HttpResponse<String> response = client.request(request, String.class);

        if (response.getStatus().getCode() != 200) {
            throw new RuntimeException("Failed to execute statement: " + response.getStatus().getCode() + " - " + response.getBody());
        }

        return extractOperationHandleFromResponse(response.getBody());
    }

    private OperationResult waitForOperationCompletion(RunContext runContext, String gatewayUrl,
                                                       String sessionHandle, String operationHandle)
            throws IOException, InterruptedException, IllegalVariableEvaluationException {

        HttpClient client = HttpClient.builder()
            .runContext(runContext)
            .build();

        int timeout = Math.max(1, runContext.render(statementTimeout).as(Integer.class).orElse(300));
        final int intervalSec = 1;
        final int maxAttempts = Math.max(1, timeout); // Check every 1 second
        final java.time.Instant deadline = java.time.Instant.now().plusSeconds(timeout);

        try {
            return new RetryUtils().<OperationResult, Exception>of(
                Exponential.builder()
                    .delayFactor(1.0) // Fixed interval
                    .interval(Duration.ofSeconds(intervalSec))
                    .maxInterval(Duration.ofSeconds(intervalSec))
                    .maxAttempts(maxAttempts)
                    .build()
            ).run(
                (result, throwable) -> {
                    if (result != null) return false;
                    if (throwable instanceof NonRetriableOperationException ||
                        throwable instanceof java.util.concurrent.TimeoutException) {
                        return false;
                    }
                    return true; // retry other exceptions
                },
                () -> {
                    // Hard-stop if global timeout elapsed
                    if (java.time.Instant.now().isAfter(deadline)) {
                        throw new java.util.concurrent.TimeoutException("Operation timed out after " + timeout + " seconds");
                    }

                    HttpRequest request = HttpRequest.builder()
                        .uri(URI.create(gatewayUrl + "/v1/sessions/" + sessionHandle + "/operations/" + operationHandle + "/status"))
                        .method("GET")
                        .build();

                    HttpResponse<String> response = client.request(request, String.class);

                    int statusCode = response.getStatus().getCode();
                    if (statusCode != 200) {
                        if (statusCode >= 500 || statusCode == 429) {
                            runContext.logger().warn("Transient status {} from Flink SQL Gateway; will retry. Body: {}", statusCode, response.getBody());
                            return null; // keep polling
                        }
                        throw new NonRetriableOperationException("Failed to get operation status: " + statusCode + " - " + response.getBody());
                    }

                    String status = extractStatusFromResponse(response.getBody());

                    // Get acceptable states
                    java.util.List<String> acceptableStates = getAcceptableStates(runContext);

                    if (acceptableStates.contains(status)) {
                        return new OperationResult(status, extractRowCountFromResponse(response.getBody()));
                    } else if ("ERROR".equals(status) || "CANCELED".equals(status)) {
                        throw new NonRetriableOperationException("Operation failed with status: " + status + " - " + response.getBody());
                    }

                    // Return null to continue polling
                    return null;
                }
            );
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }
            throw new RuntimeException("Operation monitoring failed", e);
        }
    }

    private void closeSession(RunContext runContext, String gatewayUrl, String sessionHandle) {
        try {
            HttpClient client = HttpClient.builder()
                .runContext(runContext)
                .build();

            HttpRequest request = HttpRequest.builder()
                .uri(URI.create(gatewayUrl + "/v1/sessions/" + sessionHandle))
                .method("DELETE")
                .build();

            client.request(request, String.class);
            runContext.logger().info("Closed session: {}", sessionHandle);
        } catch (Exception e) {
            runContext.logger().warn("Failed to close session: {}", sessionHandle, e);
        }
    }

    private String extractSessionHandleFromResponse(String responseBody) {
        try {
            JsonNode identifier = JSON.readTree(responseBody).path("sessionHandle").path("identifier");
            if (identifier.isTextual() && !identifier.asText().isEmpty()) {
                return identifier.asText();
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not parse session handle from response: " + responseBody, e);
        }
        throw new RuntimeException("Could not extract session handle from response: " + responseBody);
    }

    private String extractOperationHandleFromResponse(String responseBody) {
        try {
            JsonNode identifier = JSON.readTree(responseBody).path("operationHandle").path("identifier");
            if (identifier.isTextual() && !identifier.asText().isEmpty()) {
                return identifier.asText();
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not parse operation handle from response: " + responseBody, e);
        }
        throw new RuntimeException("Could not extract operation handle from response: " + responseBody);
    }

    private String extractStatusFromResponse(String responseBody) {
        try {
            JsonNode status = JSON.readTree(responseBody).path("status");
            if (status.isTextual()) {
                return status.asText();
            }
            return status.path("id").asText("UNKNOWN");
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not parse operation status from response: " + responseBody, e);
        }
    }

    private int extractRowCountFromResponse(String responseBody) {
        try {
            JsonNode node = JSON.readTree(responseBody);
            if (node.has("rowCount")) {
                return node.path("rowCount").asInt(-1);
            }
            return node.path("result").path("rowCount").asInt(-1);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not parse row count from response: " + responseBody, e);
        }
    }

    private String findSessionByName(String sessionsListBody, String sessionName) {
        try {
            JsonNode root = JSON.readTree(sessionsListBody);
            JsonNode sessions = root.path("sessions");
            if (sessions.isArray()) {
                for (JsonNode session : sessions) {
                    String name = session.path("sessionName").asText(null);
                    if (sessionName.equals(name)) {
                        return session.path("sessionHandle").path("identifier").asText(null);
                    }
                }
            }
        } catch (JsonProcessingException e) {
            // If we can't parse, we'll just create a new session
        }
        return null;
    }

    private java.util.List<String> getAcceptableStates(RunContext runContext)
            throws IllegalVariableEvaluationException {
        if (acceptableStates != null) {
            return runContext.render(acceptableStates).asList(String.class);
        }
        return java.util.Arrays.asList("FINISHED", "RUNNING");
    }

    @Builder
    @Getter
    public static class SessionConfig {
        private final String catalog;
        private final String database;
        private final Map<String, String> configuration;
    }

    @Builder
    @Getter
    public static class OperationResult {
        private final String status;
        private final int rowCount;

        public OperationResult(String status, int rowCount) {
            this.status = status;
            this.rowCount = rowCount;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The operation handle",
            description = "The unique identifier for the executed SQL operation"
        )
        private final String operationHandle;

        @Schema(
            title = "The session handle",
            description = "The unique identifier for the SQL Gateway session"
        )
        private final String sessionHandle;

        @Schema(
            title = "Result count",
            description = "Number of rows affected or returned by the operation"
        )
        private final Integer resultCount;

        @Schema(
            title = "Operation status",
            description = "Final status of the operation"
        )
        private final String status;
    }

    private static final class NonRetriableOperationException extends RuntimeException {
        NonRetriableOperationException(String message) { super(message); }
    }
}