package io.kestra.plugin.flink;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Submit a Flink job using a JAR file.",
    description = "This task submits a job to Apache Flink using a JAR file. " +
                  "It supports job configuration, parallelism settings, and savepoint restoration."
)
@Plugin(
    examples = {
        @Example(
            title = "Submit a Flink batch job",
            full = true,
            code = """
                id: flink-batch-job
                namespace: company.team

                tasks:
                  - id: submit-job
                    type: io.kestra.plugin.flink.Submit
                    restUrl: "http://flink-jobmanager:8081"
                    jarUri: "s3://flink/jars/my-batch-job.jar"
                    entryClass: "com.example.BatchJobMain"
                    args:
                      - "--input"
                      - "s3://input/data/"
                      - "--output"
                      - "s3://output/results/"
                    parallelism: 4
                """
        )
    }
)
public class Submit extends Task implements RunnableTask<Submit.Output> {

    @Schema(
        title = "Flink REST API URL",
        description = "The base URL of the Flink cluster's REST API, e.g., 'http://flink-jobmanager:8081'"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected Property<String> restUrl;

    @Schema(
        title = "URI of the JAR file to submit",
        description = "The URI pointing to the JAR file containing the Flink job. " +
                      "Supports file://, s3://, http:// and other schemes."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Property<String> jarUri;

    @Schema(
        title = "Main class to execute",
        description = "The fully qualified name of the main class to execute."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Property<String> entryClass;

    @Schema(
        title = "Program arguments",
        description = "Arguments to pass to the main method of the job."
    )
    @PluginProperty(dynamic = true)
    private Property<List<String>> args;

    @Schema(
        title = "Job parallelism",
        description = "The parallelism for the job execution. If not specified, " +
                      "the cluster default parallelism will be used."
    )
    @PluginProperty
    private Property<Integer> parallelism;

    @Schema(
        title = "Restore from savepoint",
        description = "Path to a savepoint to restore the job from."
    )
    @PluginProperty(dynamic = true)
    private Property<String> restoreFromSavepoint;

    @Schema(
        title = "Allow non-restored state",
        description = "Allow to skip savepoint state that cannot be restored. " +
                      "Defaults to false."
    )
    @PluginProperty
    @Builder.Default
    private Property<Boolean> allowNonRestoredState = Property.of(false);

    @Schema(
        title = "Job configuration",
        description = "Additional configuration parameters for the job."
    )
    @PluginProperty(dynamic = true)
    private Property<Map<String, String>> jobConfig;

    @Override
    public Submit.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rJarUri = runContext.render(this.jarUri).as(String.class).orElseThrow();
        String rEntryClass = runContext.render(this.entryClass).as(String.class).orElseThrow();
        String rRestUrl = runContext.render(this.restUrl).as(String.class).orElseThrow();

        logger.info("Submitting Flink job: {} from {}", rEntryClass, rJarUri);

        // Download JAR if needed
        URI jarLocation = downloadJar(runContext, rJarUri);

        // Upload JAR to Flink cluster
        String jarId = uploadJarToFlink(runContext, rRestUrl, jarLocation);

        // Submit job
        String jobId = submitJob(runContext, rRestUrl, jarId, rEntryClass);

        logger.info("Successfully submitted Flink job with ID: {}", jobId);

        return Output.builder()
            .jobId(jobId)
            .jarId(jarId)
            .build();
    }

    private URI downloadJar(RunContext runContext, String jarUri) throws IllegalVariableEvaluationException, IOException {
        if (jarUri.startsWith("file://")) {
            return URI.create(jarUri);
        }

        // For remote JARs, download to working directory
        try (InputStream jarStream = runContext.storage().getFile(URI.create(jarUri))) {
            java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("flink-job", ".jar");
            java.nio.file.Files.copy(jarStream, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return tempFile.toUri();
        }
    }

    private String uploadJarToFlink(RunContext runContext, String restUrl, URI jarLocation) throws Exception {
        try (HttpClient client = HttpClient.builder()
                .runContext(runContext)
                .build()) {
            java.nio.file.Path jarPath = java.nio.file.Path.of(jarLocation);
            String fileName = jarPath.getFileName().toString();

            // Create multipart request body for JAR upload
            String boundary = "----FlinkJarUpload" + System.currentTimeMillis();
            String prefix = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + fileName + "\"\r\n"
                + "Content-Type: application/java-archive\r\n\r\n";
            String suffix = "\r\n--" + boundary + "--\r\n";

            // Read jar file content and create full body
            byte[] jarContent = java.nio.file.Files.readAllBytes(jarPath);
            byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
            byte[] suffixBytes = suffix.getBytes(StandardCharsets.UTF_8);

            byte[] fullBody = new byte[prefixBytes.length + jarContent.length + suffixBytes.length];
            System.arraycopy(prefixBytes, 0, fullBody, 0, prefixBytes.length);
            System.arraycopy(jarContent, 0, fullBody, prefixBytes.length, jarContent.length);
            System.arraycopy(suffixBytes, 0, fullBody, prefixBytes.length + jarContent.length, suffixBytes.length);

            HttpRequest request = HttpRequest.builder()
                .uri(URI.create(restUrl + "/v1/jars/upload"))
                .method("POST")
                .addHeader("Content-Type", "multipart/form-data; boundary=" + boundary)
                .body(HttpRequest.ByteArrayRequestBody.builder()
                    .content(fullBody)
                    .build())
                .build();

            HttpResponse<String> response = client.request(request, String.class);

            if (response.getStatus().getCode() != 200) {
                throw new RuntimeException("Failed to upload JAR: " + response.getStatus().getCode() + " - " + response.getBody());
            }

            // Parse response to get JAR ID
            // Expected response format: {"filename": "...", "status": "success"}
            String responseBody = response.getBody();
            String jarId = extractJarIdFromResponse(responseBody);

            runContext.logger().info("Uploaded JAR with ID: {}", jarId);
            return jarId;
        }
    }

    private String submitJob(RunContext runContext, String restUrl, String jarId, String entryClass) throws Exception {
        try (HttpClient client = HttpClient.builder()
                .runContext(runContext)
                .build()) {

        // Build job submission payload
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("entryClass", entryClass);

        if (parallelism != null) {
            Integer parallelismValue = runContext.render(parallelism).as(Integer.class).orElse(null);
            if (parallelismValue != null) {
                payload.put("parallelism", parallelismValue);
            }
        }

        if (args != null) {
            List<String> renderedArgs = runContext.render(args).asList(String.class);
            if (renderedArgs != null && !renderedArgs.isEmpty()) {
                payload.put("programArgs", String.join(" ", renderedArgs));
            }
        }

        if (restoreFromSavepoint != null) {
            String savepointPath = runContext.render(restoreFromSavepoint).as(String.class).orElse(null);
            if (savepointPath != null) {
                payload.put("savepointPath", savepointPath);
                Boolean allowNonRestored = runContext.render(allowNonRestoredState).as(Boolean.class).orElse(false);
                payload.put("allowNonRestoredState", allowNonRestored);
            }
        }

        if (jobConfig != null) {
            Map<String, String> config = runContext.render(jobConfig).asMap(String.class, String.class);
            if (config != null && !config.isEmpty()) {
                payload.put("flinkConfiguration", config);
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        String requestBody = mapper.writeValueAsString(payload);

            HttpRequest request = HttpRequest.builder()
                .uri(URI.create(restUrl + "/v1/jars/" + jarId + "/run"))
                .method("POST")
                .addHeader("Content-Type", "application/json")
                .body(HttpRequest.StringRequestBody.builder()
                    .content(requestBody)
                    .build())
                .build();

            HttpResponse<String> response = client.request(request, String.class);

            if (response.getStatus().getCode() != 200) {
                throw new RuntimeException("Failed to submit job: " + response.getStatus().getCode() + " - " + response.getBody());
            }

            // Extract job ID from response
            String jobId = extractJobIdFromResponse(response.getBody());
            return jobId;
        }
    }

    private String extractJarIdFromResponse(String responseBody) {
        Matcher matcher = Pattern.compile("\"filename\"\\s*:\\s*\"([^\"]+)\"").matcher(responseBody);
        if (matcher.find()) {
            String filename = matcher.group(1);
            int separator = filename.lastIndexOf('/');
            return separator >= 0 ? filename.substring(separator + 1) : filename;
        }
        throw new RuntimeException("Could not extract JAR ID from response: " + responseBody);
    }

    private String extractJobIdFromResponse(String responseBody) {
        Matcher matcher = Pattern.compile("\"jobid\"\\s*:\\s*\"([^\"]+)\"").matcher(responseBody);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new RuntimeException("Could not extract job ID from response: " + responseBody);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The Flink job ID",
            description = "The unique identifier assigned to the submitted job"
        )
        private final String jobId;

        @Schema(
            title = "The JAR ID on the Flink cluster",
            description = "The identifier of the uploaded JAR on the Flink cluster"
        )
        private final String jarId;
    }
}