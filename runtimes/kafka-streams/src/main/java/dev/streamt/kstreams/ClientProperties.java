package dev.streamt.kstreams;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.streams.StreamsConfig;

/** Runtime-only connection settings. Plan/identity/delivery settings cannot be overridden. */
public final class ClientProperties {
    public static final Set<String> ALLOWED = Set.of(
        "bootstrap.servers", "security.protocol", "sasl.mechanism", "sasl.jaas.config",
        "ssl.truststore.location", "ssl.truststore.password", "ssl.truststore.type",
        "ssl.keystore.location", "ssl.keystore.password", "ssl.keystore.type", "ssl.key.password",
        "ssl.truststore.certificates", "ssl.keystore.certificate.chain", "ssl.keystore.key",
        "ssl.endpoint.identification.algorithm", "request.timeout.ms", "connections.max.idle.ms",
        "socket.connection.setup.timeout.ms", "socket.connection.setup.timeout.max.ms");
    private static final int MAX_FILE_BYTES = 1_048_576;

    private ClientProperties() { }

    public static Properties read(Path path) throws IOException {
        byte[] bytes;
        try (var input = Files.newInputStream(path)) { bytes = input.readNBytes(MAX_FILE_BYTES + 1); }
        require(bytes.length <= MAX_FILE_BYTES, "client_properties_too_large");
        String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        return parse(text);
    }

    public static Properties parse(String text) throws IOException {
        Properties settings = new Properties() {
            @Override public synchronized Object put(Object key, Object value) {
                require(!containsKey(key), "duplicate_client_property");
                return super.put(key, value);
            }
        };
        settings.load(new StringReader(text));
        for (String key : settings.stringPropertyNames()) {
            require(ALLOWED.contains(key), "unsupported_client_property");
            require(!settings.getProperty(key).isBlank(), "empty_client_property");
        }
        String bootstrap = settings.getProperty("bootstrap.servers", "");
        require(!bootstrap.isBlank(), "bootstrap_servers_required");
        for (String address : bootstrap.split(",", -1)) {
            require(address.matches("(?:[A-Za-z0-9][A-Za-z0-9.-]*|\\[[0-9A-Fa-f:]+\\]):[0-9]{1,5}"), "invalid_bootstrap_servers");
            int port = Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
            require(port > 0 && port <= 65535, "invalid_bootstrap_servers");
        }
        String protocol = settings.getProperty("security.protocol", "PLAINTEXT");
        require(Set.of("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL").contains(protocol), "unsupported_security_protocol");
        if (!settings.containsKey("security.protocol")) settings.setProperty("security.protocol", protocol);
        if (protocol.startsWith("SASL_")) validateSasl(settings);
        else require(!settings.containsKey("sasl.mechanism") && !settings.containsKey("sasl.jaas.config"), "sasl_requires_sasl_protocol");
        if (protocol.endsWith("SSL")) validateTls(settings);
        else require(settings.stringPropertyNames().stream().noneMatch(key -> key.startsWith("ssl.")), "tls_requires_ssl_protocol");
        for (String key : Set.of("request.timeout.ms", "connections.max.idle.ms",
                "socket.connection.setup.timeout.ms", "socket.connection.setup.timeout.max.ms")) {
            if (settings.containsKey(key)) {
                try {
                    long value = Long.parseLong(settings.getProperty(key));
                    require(value >= 1 && value <= 300_000, "invalid_client_timeout");
                } catch (NumberFormatException invalid) { throw new IllegalArgumentException("invalid_client_timeout"); }
            }
        }
        // Return a normal defensive copy; the duplicate-checking loader is read-only after parse.
        Properties copy = new Properties();
        copy.putAll(settings);
        return copy;
    }

    private static void validateSasl(Properties settings) {
        String mechanism = settings.getProperty("sasl.mechanism", "");
        require(Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512").contains(mechanism), "unsupported_sasl_mechanism");
        require(settings.containsKey("sasl.jaas.config"), "sasl_jaas_required");
        try {
            var entries = JaasContext.loadClientContext(Map.of("sasl.jaas.config", new Password(settings.getProperty("sasl.jaas.config")))).configurationEntries();
            require(entries.size() == 1, "invalid_sasl_jaas");
            var entry = entries.get(0);
            String module = mechanism.equals("PLAIN") ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                : "org.apache.kafka.common.security.scram.ScramLoginModule";
            require(entry.getLoginModuleName().equals(module), "invalid_sasl_jaas");
            require(entry.getControlFlag() == AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, "invalid_sasl_jaas");
            require(entry.getOptions().keySet().equals(Set.of("username", "password")), "invalid_sasl_jaas");
            require(entry.getOptions().get("username") instanceof String && entry.getOptions().get("password") instanceof String, "invalid_sasl_jaas");
        } catch (RuntimeException invalid) {
            // Kafka parsing errors can include JAAS text; never propagate them to the entrypoint.
            throw new IllegalArgumentException("invalid_sasl_jaas");
        }
    }

    private static void validateTls(Properties settings) {
        require(settings.getProperty("ssl.endpoint.identification.algorithm", "https").equals("https"), "tls_hostname_verification_required");
        settings.putIfAbsent("ssl.endpoint.identification.algorithm", "https");
        for (String store : Set.of("truststore", "keystore")) {
            String type = settings.getProperty("ssl." + store + ".type", "JKS");
            require(Set.of("JKS", "PKCS12", "PEM").contains(type), "unsupported_tls_store_type");
            boolean inline = store.equals("truststore") ? settings.containsKey("ssl.truststore.certificates")
                : settings.containsKey("ssl.keystore.key") || settings.containsKey("ssl.keystore.certificate.chain");
            if (inline) {
                require(type.equals("PEM") && !settings.containsKey("ssl." + store + ".location"), "conflicting_tls_store_settings");
                if (store.equals("keystore")) require(settings.containsKey("ssl.keystore.key")
                    && settings.containsKey("ssl.keystore.certificate.chain"), "incomplete_pem_keystore");
            }
            if (type.equals("PEM")) require(!settings.containsKey("ssl." + store + ".password"), "pem_store_password_unsupported");
            if (settings.containsKey("ssl." + store + ".location")) {
                require(Path.of(settings.getProperty("ssl." + store + ".location")).isAbsolute(), "tls_store_path_must_be_absolute");
            }
        }
    }

    public static Properties streams(Properties connection, String applicationId, Path stateDirectory) {
        require(applicationId.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,199}"), "invalid_application_id");
        Properties settings = new Properties();
        settings.putAll(connection);
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        settings.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
        settings.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        settings.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        settings.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "none");
        settings.put(StreamsConfig.consumerPrefix("isolation.level"), "read_committed");
        settings.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_GLOBAL_ENABLED_CONFIG, true);
        settings.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, "org.apache.kafka.streams.errors.LogAndFailExceptionHandler");
        settings.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, "org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler");
        return settings;
    }

    /** Validate local certificate/key material without a Kafka client, DNS lookup or handshake. */
    public static void validateLocalTls(Properties connection) {
        if (!connection.getProperty("security.protocol", "PLAINTEXT").endsWith("SSL")) return;
        try (SslFactory ssl = new SslFactory(ConnectionMode.CLIENT)) {
            ssl.configure(new AdminClientConfig(connection).values());
        } catch (RuntimeException invalid) {
            throw new IllegalArgumentException("invalid_local_tls_material");
        }
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new IllegalArgumentException(reason);
    }
}
