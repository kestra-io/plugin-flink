package io.kestra.plugin.flink;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoHeaders;
import org.apache.flink.runtime.rest.messages.ConfigurationInfo;
import org.apache.flink.runtime.rest.messages.ConfigurationInfoEntry;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.slf4j.Logger;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

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
    description = "Uploads a JAR to the Flink REST API to retain its artifact ID, then submits the job with the Flink Java client. Supports program arguments, explicit parallelism, and savepoint restore. The compatibility upload buffers large JARs in memory. HTTPS uses the JVM default trust configuration; custom truststores and mutual TLS are not configurable here."
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
        description = "Arguments passed to the main method; rendered then space-joined."
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
        String rRestUrl = normalizeRestUrl(runContext.render(this.restUrl).as(String.class).orElseThrow());

        logger.info("Submitting Flink job: {} from {}", rEntryClass, rJarUri);

        // Download JAR if needed
        URI jarLocation = downloadJar(runContext, rJarUri);

        // Upload JAR to Flink cluster
        String jarId = uploadJarToFlink(runContext, rRestUrl, jarLocation);

        // Submit job
        String jobId = submitJob(runContext, rRestUrl, jarLocation, rEntryClass);

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
            java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("flink-job", ".jar");
            java.nio.file.Files.copy(jarStream, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            tempFile.toFile().deleteOnExit();
            return tempFile.toUri();
        }
    }

    private String uploadJarToFlink(RunContext runContext, String restUrl, URI jarLocation) throws Exception {
        try (
            HttpClient client = HttpClient.builder()
                .runContext(runContext)
                .build()
        ) {
            java.nio.file.Path jarPath = java.nio.file.Paths.get(jarLocation);
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
                .uri(URI.create(restEndpoint(restUrl, "/v1/jars/upload")))
                .method("POST")
                .addHeader("Content-Type", "multipart/form-data; boundary=" + boundary)
                .body(
                    HttpRequest.ByteArrayRequestBody.builder()
                        .content(fullBody)
                        .build()
                )
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

    private String submitJob(RunContext runContext, String restUrl, URI jarLocation, String entryClass) throws Exception {
        Configuration configuration = createConfiguration(restUrl);
        Configuration jobConfiguration = new Configuration();
        Configuration programConfiguration = new Configuration(configuration);
        if (jobConfig != null) {
            Map<String, String> config = runContext.render(jobConfig).asMap(String.class, String.class);
            if (config != null && !config.isEmpty()) {
                config.forEach((key, value) ->
                {
                    jobConfiguration.setString(key, value);
                    programConfiguration.setString(key, value);
                });
            }
        }

        int defaultParallelism;
        if (parallelism != null) {
            Integer parallelismValue = runContext.render(parallelism).as(Integer.class).orElse(null);
            if (parallelismValue != null) {
                defaultParallelism = parallelismValue;
            } else if (jobConfiguration.contains(CoreOptions.DEFAULT_PARALLELISM)) {
                defaultParallelism = jobConfiguration.get(CoreOptions.DEFAULT_PARALLELISM);
            } else {
                defaultParallelism = getClusterDefaultParallelism(restUrl);
            }
        } else if (jobConfiguration.contains(CoreOptions.DEFAULT_PARALLELISM)) {
            defaultParallelism = jobConfiguration.get(CoreOptions.DEFAULT_PARALLELISM);
        } else {
            defaultParallelism = getClusterDefaultParallelism(restUrl);
        }

        PackagedProgram.Builder programBuilder = PackagedProgram.newBuilder()
            .setJarFile(java.nio.file.Paths.get(jarLocation).toFile())
            .setEntryPointClassName(entryClass)
            .setConfiguration(programConfiguration);

        if (args != null) {
            List<String> renderedArgs = runContext.render(args).asList(String.class);
            if (renderedArgs != null && !renderedArgs.isEmpty()) {
                programBuilder.setArguments(renderedArgs.toArray(new String[0]));
            }
        }

        if (restoreFromSavepoint != null) {
            String savepointPath = runContext.render(restoreFromSavepoint).as(String.class).orElse(null);
            if (savepointPath != null) {
                Boolean allowNonRestored = runContext.render(allowNonRestoredState).as(Boolean.class).orElse(false);
                programBuilder.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath, allowNonRestored));
            }
        }

        try (
            PackagedProgram program = programBuilder.build();
            RestClusterClient<StandaloneClusterId> client = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance())
        ) {
            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, programConfiguration, defaultParallelism, true);
            jobGraph.setJobConfiguration(jobConfiguration);

            // submitJob is asynchronous; wait for Flink's assigned job ID before returning the task output.
            JobID jobId;
            try {
                jobId = client.submitJob(jobGraph).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            return jobId.toString();
        }
    }

    private Configuration createConfiguration(String restUrl) {
        URI restUri = URI.create(restUrl);
        String scheme = restUri.getScheme();
        if (restUri.getHost() == null || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException("restUrl must be an HTTP or HTTPS URL with a host: " + restUrl);
        }

        Configuration configuration = new Configuration();
        configuration.set(RestOptions.ADDRESS, restUri.getHost());
        int port = restUri.getPort();
        if (port < 0) {
            port = RestOptions.PORT.defaultValue();
        }
        configuration.set(RestOptions.PORT, port);

        String path = restUri.getPath();
        if (path != null) {
            path = path.replaceFirst("/+$", "");
        }
        if (path != null && !path.isEmpty() && !"/".equals(path)) {
            configuration.set(RestOptions.PATH, path);
        }
        if ("https".equalsIgnoreCase(scheme)) {
            configuration.set(SecurityOptions.SSL_REST_ENABLED, true);
        }
        return configuration;
    }

    private String normalizeRestUrl(String restUrl) {
        URI restUri = URI.create(restUrl);
        String scheme = restUri.getScheme();
        if (restUri.getHost() == null || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException("restUrl must be an HTTP or HTTPS URL with a host: " + restUrl);
        }
        if (restUri.getRawQuery() != null || restUri.getRawFragment() != null) {
            throw new IllegalArgumentException("restUrl must not include query parameters or a fragment: " + restUrl);
        }
        if (restUri.getPort() >= 0) {
            return restUrl;
        }

        StringBuilder normalizedUrl = new StringBuilder()
            .append(scheme)
            .append("://")
            .append(restUri.getRawAuthority())
            .append(':')
            .append(RestOptions.PORT.defaultValue());
        String path = restUri.getRawPath();
        if (path != null) {
            path = path.replaceFirst("/+$", "");
            normalizedUrl.append(path);
        }
        return normalizedUrl.toString();
    }

    private String restEndpoint(String restUrl, String endpoint) {
        return restUrl.replaceFirst("/+$", "") + endpoint;
    }

    private int getClusterDefaultParallelism(String restUrl) throws Exception {
        Configuration clusterConfiguration = new Configuration();
        try (
            RestClusterClient<StandaloneClusterId> client = new RestClusterClient<>(
                createConfiguration(restUrl),
                StandaloneClusterId.getInstance()
            )
        ) {
            ConfigurationInfo configuration = client.sendRequest(
                ClusterConfigurationInfoHeaders.getInstance(),
                EmptyMessageParameters.getInstance(),
                EmptyRequestBody.getInstance()
            ).get();
            for (ConfigurationInfoEntry entry : configuration) {
                if (CoreOptions.DEFAULT_PARALLELISM.key().equals(entry.getKey())) {
                    clusterConfiguration.setString(CoreOptions.DEFAULT_PARALLELISM.key(), entry.getValue());
                    break;
                }
            }
        }
        return clusterConfiguration.get(CoreOptions.DEFAULT_PARALLELISM);
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
