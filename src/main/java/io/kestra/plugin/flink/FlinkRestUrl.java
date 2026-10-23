package io.kestra.plugin.flink;

import java.net.URI;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;

final class FlinkRestUrl {
    private FlinkRestUrl() {
    }

    static URI validate(String restUrl) {
        var restUri = URI.create(restUrl);
        var scheme = restUri.getScheme();
        if (restUri.getHost() == null || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException("restUrl must be an HTTP or HTTPS URL with a host: " + restUrl);
        }
        if (restUri.getRawQuery() != null || restUri.getRawFragment() != null) {
            throw new IllegalArgumentException("restUrl must not include query parameters or a fragment: " + restUrl);
        }
        return restUri;
    }

    static String normalize(String restUrl) {
        var restUri = validate(restUrl);
        if (restUri.getPort() >= 0) {
            return restUrl;
        }
        var path = restUri.getRawPath();
        return restUri.getScheme() + "://" + restUri.getRawAuthority() + ":" + RestOptions.PORT.defaultValue()
            + (path == null ? "" : path);
    }

    static String endpoint(String restUrl, String endpoint) {
        return normalize(restUrl).replaceFirst("/+$", "") + endpoint;
    }

    static Configuration configuration(String restUrl) {
        var restUri = validate(restUrl);
        var configuration = new Configuration();
        configuration.set(RestOptions.ADDRESS, restUri.getHost());
        configuration.set(RestOptions.PORT, restUri.getPort() < 0 ? RestOptions.PORT.defaultValue() : restUri.getPort());

        var path = restUri.getRawPath();
        if (path != null) {
            path = path.replaceFirst("/+$", "");
        }
        if (path != null && !path.isEmpty()) {
            configuration.set(RestOptions.PATH, path);
        }
        if ("https".equalsIgnoreCase(restUri.getScheme())) {
            configuration.set(SecurityOptions.SSL_REST_ENABLED, true);
        }
        return configuration;
    }
}
