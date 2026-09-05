package dev.streamt.kstreams;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class ClientPropertiesTest {
    private static final String BASE = "bootstrap.servers=broker:9092\n";

    @Test void fixedDeliveryIdentityAndOffsets() throws Exception {
        var config = ClientProperties.streams(ClientProperties.parse(BASE), "orders-clean", Path.of("/state"));
        assertEquals("none", config.getProperty("consumer.auto.offset.reset"));
        assertEquals("exactly_once_v2", config.getProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        assertEquals("orders-clean", config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("read_committed", config.getProperty("consumer.isolation.level"));
    }

    @ParameterizedTest @ValueSource(strings = {"application.id", "group.id", "auto.offset.reset", "consumer.auto.offset.reset",
        "processing.guarantee", "consumer.isolation.level", "state.dir", "num.stream.threads", "config.providers",
        "sasl.client.callback.handler.class", "key.deserializer", "default.value.serde", "metric.reporters", "transactional.id"})
    void cannotOverrideExecutionContract(String key) {
        assertThrows(IllegalArgumentException.class, () -> ClientProperties.parse(BASE + key + "=secret\n"));
    }

    @ParameterizedTest @ValueSource(strings = {"", "bootstrap.servers=http://user:secret@broker:9092", "bootstrap.servers=broker:0",
        "bootstrap.servers=broker:65536", "bootstrap.servers=broker:9092,", "bootstrap.servers=broker:9092\nbootstrap.servers=other:9092",
        "bootstrap.servers=broker:9092\nboot\\u0073trap.servers=other:9092", "bootstrap.servers=broker:9092\nrequest.timeout.ms=0",
        "bootstrap.servers=broker:9092\nsecurity.protocol=UNSUPPORTED", "bootstrap.servers=broker:9092\nsasl.mechanism=PLAIN"})
    void rejectsMalformedOrAmbiguousProperties(String properties) {
        assertThrows(IllegalArgumentException.class, () -> ClientProperties.parse(properties));
    }

    @ParameterizedTest @ValueSource(strings = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"})
    void parsesSupportedJaasWithoutEchoingCredentials(String mechanism) throws Exception {
        String module = mechanism.equals("PLAIN") ? "plain.PlainLoginModule" : "scram.ScramLoginModule";
        var settings = ClientProperties.parse(BASE + "security.protocol=SASL_SSL\nsasl.mechanism=" + mechanism
            + "\nsasl.jaas.config=org.apache.kafka.common.security." + module + " required username=\"user\" password=\"secret\";\n");
        assertEquals("https", settings.getProperty("ssl.endpoint.identification.algorithm"));
        assertTrue(settings.getProperty("sasl.jaas.config").contains("secret"));
    }

    @ParameterizedTest @ValueSource(strings = {
        "other.LoginModule required username=\"user\" password=\"secret\";",
        "org.apache.kafka.common.security.plain.PlainLoginModule optional username=\"user\" password=\"secret\";",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"secret\" extra=\"value\";",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"secret\"",
        "bad-secret-value"
    }) void rejectsUnapprovedJaasWithoutLeakingIt(String jaas) {
        var error = assertThrows(IllegalArgumentException.class, () -> ClientProperties.parse(BASE
            + "security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=" + jaas));
        assertEquals("invalid_sasl_jaas", error.getMessage());
        assertNull(error.getCause());
    }

    @Test void pemValuesUseJavaPropertiesNewlineEscapes() throws Exception {
        var settings = ClientProperties.parse(BASE + "security.protocol=SSL\nssl.truststore.type=PEM\n"
            + "ssl.truststore.certificates=-----BEGIN CERTIFICATE-----\\nexample\\n-----END CERTIFICATE-----\n");
        assertTrue(settings.getProperty("ssl.truststore.certificates").contains("\nexample\n"));
        assertThrows(IllegalArgumentException.class, () -> ClientProperties.validateLocalTls(settings));
    }

    @Test void localTlsValidationNeedsNoReachableBroker() throws Exception {
        var settings = ClientProperties.parse("bootstrap.servers=not-resolvable.invalid:9092\nsecurity.protocol=SSL\n");
        assertDoesNotThrow(() -> ClientProperties.validateLocalTls(settings));
    }

    @Test void missingStoreRejectedBeforeClientConstruction() throws Exception {
        var settings = ClientProperties.parse(BASE + "security.protocol=SSL\nssl.truststore.location=/nonexistent/secret.jks\n");
        var error = assertThrows(IllegalArgumentException.class, () -> ClientProperties.validateLocalTls(settings));
        assertEquals("invalid_local_tls_material", error.getMessage());
        assertNull(error.getCause());
    }

    @ParameterizedTest @ValueSource(strings = {"ssl.endpoint.identification.algorithm=", "ssl.endpoint.identification.algorithm=none",
        "ssl.truststore.location=relative.jks", "ssl.truststore.type=OTHER",
        "ssl.truststore.type=PEM\nssl.truststore.password=secret",
        "ssl.truststore.type=PEM\nssl.truststore.location=/file\nssl.truststore.certificates=inline",
        "ssl.keystore.type=PEM\nssl.keystore.key=secret", "ssl.truststore.certificates=inline"})
    void rejectsUnsafeOrConflictingTlsProperties(String fields) {
        assertThrows(IllegalArgumentException.class, () -> ClientProperties.parse(BASE + "security.protocol=SSL\n" + fields));
    }
}
