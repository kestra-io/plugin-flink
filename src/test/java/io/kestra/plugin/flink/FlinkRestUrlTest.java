package io.kestra.plugin.flink;

import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkRestUrlTest {
    @Test
    void normalizeAddsDefaultPortAndPreservesRawPath() {
        var normalized = FlinkRestUrl.normalize("http://flink.example/base%2Fpath///");

        assertEquals(
            "http://flink.example:" + RestOptions.PORT.defaultValue() + "/base%2Fpath///",
            normalized
        );
    }

    @Test
    void endpointAddsDefaultPortAndRemovesTrailingSlashes() {
        var endpoint = FlinkRestUrl.endpoint("https://flink.example/base///", "/v1/jobs");

        assertEquals(
            "https://flink.example:" + RestOptions.PORT.defaultValue() + "/base/v1/jobs",
            endpoint
        );
    }

    @Test
    void endpointPreservesExplicitPort() {
        assertEquals(
            "http://flink.example:1234/base/v1/jobs",
            FlinkRestUrl.endpoint("http://flink.example:1234/base///", "/v1/jobs")
        );
    }

    @Test
    void configurationPreservesFlinkRestSettings() {
        var configuration = FlinkRestUrl.configuration("https://flink.example/base%2Fpath///");

        assertEquals("flink.example", configuration.get(RestOptions.ADDRESS));
        assertEquals(RestOptions.PORT.defaultValue(), configuration.get(RestOptions.PORT));
        assertEquals("/base%2Fpath", configuration.get(RestOptions.PATH));
        assertTrue(configuration.get(SecurityOptions.SSL_REST_ENABLED));
    }

    @Test
    void configurationPreservesOrdinaryPathAndRemovesTrailingSlashes() {
        var configuration = FlinkRestUrl.configuration("http://flink.example/base/path///");

        assertEquals("/base/path", configuration.get(RestOptions.PATH));
    }

    @Test
    void validateRejectsUnsupportedUrlsAndQueryOrFragment() {
        assertThrows(IllegalArgumentException.class, () -> FlinkRestUrl.validate("ftp://flink.example"));
        assertThrows(IllegalArgumentException.class, () -> FlinkRestUrl.validate("http:///missing-host"));
        assertThrows(IllegalArgumentException.class, () -> FlinkRestUrl.validate("http://flink.example?mode=test"));
        assertThrows(IllegalArgumentException.class, () -> FlinkRestUrl.validate("http://flink.example#fragment"));
    }
}
