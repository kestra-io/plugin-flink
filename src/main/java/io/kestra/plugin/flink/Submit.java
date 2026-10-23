package io.kestra.plugin.flink;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Submit Flink job from JAR",
    description = "Uploads a JAR to the Flink REST API and submits it through the cluster-side JAR run endpoint, so the job entry point runs on the Flink cluster rather than the Kestra worker. Supports program arguments, explicit parallelism, job configuration, and savepoint restore. The upload buffers large JARs in memory. HTTPS uses the JVM default trust configuration; custom truststores and mutual TLS are not configurable here."
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

                # The JAR must exist at jarUri and contain the specified entryClass.
                # Arguments must be valid for that class; the Flink cluster must be reachable.
                # Configure credentials and filesystem support for the JAR's storage in the Kestra runtime.
                """
        )
    }
)
public class Submit extends Task implements RunnableTask<Submit.Output> {
    private static final ObjectMapper JSON = JacksonMapper.ofJson();

    @Schema(
        title = "Flink REST API URL",
        description = "Base URL of the Flink cluster REST API (e.g., `http://flink-jobmanager:8081`). HTTPS uses the JVM default trust configuration; custom truststores and mutual TLS are not configurable here."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> restUrl;

    @Schema(
        title = "JAR URI",
        description = "URI of the job JAR to upload. Supports file://, kestra://, s3://, http:// and other schemes. Large artifacts are read fully into memory during upload."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> jarUri;

    @Schema(
        title = "Main class to execute",
        description = "Fully qualified entry class inside the JAR."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> entryClass;

    @Schema(
        title = "Program arguments",
        description = "Arguments passed to the Flink job's main method as a rendered list."
    )
    @PluginProperty(group = "advanced")
    private Property<List<String>> args;

    @Schema(
        title = "Job parallelism",
        description = "Parallelism to use for this job; defaults to the Flink cluster setting when absent."
    )
    @PluginProperty(group = "execution")
    private Property<Integer> parallelism;

    @Schema(
        title = "Restore from savepoint",
        description = "Path to a savepoint used to start the job statefully."
    )
    @PluginProperty(group = "source")
    private Property<String> restoreFromSavepoint;

    @Schema(
        title = "Allow non-restored state",
        description = "Skip savepoint state that cannot be restored; defaults to false and used only when restoreFromSavepoint is set."
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<Boolean> allowNonRestoredState = Property.ofValue(false);

    @Schema(
        title = "Job configuration",
        description = "Additional Flink configuration key/values merged into the run request."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> jobConfig;

    @Override
    public Submit.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rJarUri = runContext.render(this.jarUri).as(String.class).orElseThrow();
        String rEntryClass = runContext.render(this.entryClass).as(String.class).orElseThrow();
        var rRestUrl = FlinkRestUrl.normalize(runContext.render(this.restUrl).as(String.class).orElseThrow());

        logger.info("Submitting Flink job: {} from {}", rEntryClass, rJarUri);

        // Download JAR if needed
        var jarLocation = downloadJar(runContext, rJarUri);

        // Upload JAR to Flink cluster
        var jarId = uploadJarToFlink(runContext, rRestUrl, jarLocation);

        // Submit job to the Flink cluster
        var jobId = submitJob(runContext, rRestUrl, jarId, rEntryClass);

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

        // For remote JARs (including kestra://), download to working directory
        try (InputStream jarStream = runContext.storage().getFile(URI.create(jarUri))) {
            var tempFile = java.nio.file.Files.createTempFile("flink-job", ".jar");
            java.nio.file.Files.copy(jarStream, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            tempFile.toFile().deleteOnExit();
            return tempFile.toUri();
        }
    }

    private String uploadJarToFlink(RunContext runContext, String restUrl, URI jarLocation) throws Exception {
        try (
            var client = HttpClient.builder()
                .runContext(runContext)
                .build()
        ) {
            var jarPath = java.nio.file.Paths.get(jarLocation);
            var fileName = jarPath.getFileName().toString();

            // Create multipart request body for JAR upload
            var boundary = "----FlinkJarUpload" + System.currentTimeMillis();
            var prefix = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + fileName + "\"\r\n"
                + "Content-Type: application/java-archive\r\n\r\n";
            var suffix = "\r\n--" + boundary + "--\r\n";

            // Read jar file content and create full body
            var jarContent = java.nio.file.Files.readAllBytes(jarPath);
            var prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
            var suffixBytes = suffix.getBytes(StandardCharsets.UTF_8);

            var fullBody = new byte[prefixBytes.length + jarContent.length + suffixBytes.length];
            System.arraycopy(prefixBytes, 0, fullBody, 0, prefixBytes.length);
            System.arraycopy(jarContent, 0, fullBody, prefixBytes.length, jarContent.length);
            System.arraycopy(suffixBytes, 0, fullBody, prefixBytes.length + jarContent.length, suffixBytes.length);

            var request = HttpRequest.builder()
                .uri(URI.create(FlinkRestUrl.endpoint(restUrl, "/v1/jars/upload")))
                .method("POST")
                .addHeader("Content-Type", "multipart/form-data; boundary=" + boundary)
                .body(
                    HttpRequest.ByteArrayRequestBody.builder()
                        .content(fullBody)
                        .build()
                )
                .build();

            var response = client.request(request, String.class);

            if (response.getStatus().getCode() != 200) {
                throw new RuntimeException("Failed to upload JAR: " + response.getStatus().getCode() + " - " + response.getBody());
            }

            // Parse response to get JAR ID
            // Expected response format: {"filename": "...", "status": "success"}
            var responseBody = response.getBody();
            var jarId = extractJarIdFromResponse(responseBody);

            runContext.logger().info("Uploaded JAR with ID: {}", jarId);
            return jarId;
        }
    }

    private String submitJob(RunContext runContext, String restUrl, String jarId, String entryClass) throws Exception {
        try (
            var client = HttpClient.builder()
                .runContext(runContext)
                .build()
        ) {
            var payload = new LinkedHashMap<String, Object>();
            payload.put("entryClass", entryClass);

            if (parallelism != null) {
                var parallelismValue = runContext.render(parallelism)
                    .as(Integer.class)
                    .orElse(null);

                if (parallelismValue != null) {
                    payload.put("parallelism", parallelismValue);
                }
            }

            if (args != null) {
                var renderedArgs = runContext.render(args)
                    .asList(String.class);

                if (renderedArgs != null && !renderedArgs.isEmpty()) {
                    payload.put("programArgsList", renderedArgs);
                }
            }

            if (restoreFromSavepoint != null) {
                var savepointPath = runContext.render(restoreFromSavepoint)
                    .as(String.class)
                    .orElse(null);

                if (savepointPath != null) {
                    payload.put("savepointPath", savepointPath);

                    var allowNonRestored = runContext.render(allowNonRestoredState)
                        .as(Boolean.class)
                        .orElse(false);

                    payload.put("allowNonRestoredState", allowNonRestored);
                }
            }

            if (jobConfig != null) {
                var config = runContext.render(jobConfig)
                    .asMap(String.class, String.class);

                if (config != null && !config.isEmpty()) {
                    payload.put("flinkConfiguration", config);
                }
            }

            var requestBody = JSON.writeValueAsString(payload);

            var request = HttpRequest.builder()
                .uri(URI.create(FlinkRestUrl.endpoint(restUrl, "/v1/jars/" + jarId + "/run")))
                .method("POST")
                .addHeader("Content-Type", "application/json")
                .body(
                    HttpRequest.StringRequestBody.builder()
                        .content(requestBody)
                        .build()
                )
                .build();

            var response = client.request(request, String.class);

            if (response.getStatus().getCode() != 200) {
                throw new RuntimeException(
                    "Failed to submit job: "
                        + response.getStatus().getCode()
                        + " - "
                        + response.getBody()
                );
            }

            return extractJobIdFromResponse(response.getBody());
        }
    }

    private String extractJarIdFromResponse(String responseBody) {
        var matcher = Pattern.compile("\"filename\"\\s*:\\s*\"([^\"]+)\"").matcher(responseBody);
        if (matcher.find()) {
            var filename = matcher.group(1);
            var separator = filename.lastIndexOf('/');
            return separator >= 0 ? filename.substring(separator + 1) : filename;
        }
        throw new RuntimeException("Could not extract JAR ID from response: " + responseBody);
    }

    private String extractJobIdFromResponse(String responseBody) {
        var matcher = Pattern.compile("\"jobid\"\\s*:\\s*\"([^\"]+)\"").matcher(responseBody);

        if (matcher.find()) {
            return matcher.group(1);
        }

        throw new RuntimeException("Could not extract job ID from response: " + responseBody);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Flink job ID",
            description = "Identifier returned by Flink for the submitted job."
        )
        private final String jobId;

        @Schema(
            title = "Uploaded JAR ID",
            description = "Identifier of the uploaded JAR on the Flink cluster."
        )
        private final String jarId;
    }
}
