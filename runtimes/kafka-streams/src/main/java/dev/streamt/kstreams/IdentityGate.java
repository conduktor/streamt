package dev.streamt.kstreams;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;

/** A read-only gate on the runner's own Kafka endpoint, before any Streams client exists. */
public final class IdentityGate {
    static final int TIMEOUT_MS = 10_000;
    static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(2);

    public record Expected(String clusterId, String inputTopicId, String outputTopicId) {
        public Expected {
            require(clusterId != null && clusterId.matches("[A-Za-z0-9_-]{1,200}"));
            require(canonicalTopicId(inputTopicId) && canonicalTopicId(outputTopicId));
            require(!inputTopicId.equals(outputTopicId));
        }
    }

    interface Connection {
        KafkaFuture<String> clusterId(int timeoutMs);
        KafkaFuture<Map<String, TopicDescription>> topics(Set<String> names, int timeoutMs);
        void close(Duration timeout);
    }

    private IdentityGate() { }

    public static Expected verify(Properties properties, Plan plan, Expected expected) {
        return verify(properties, plan, expected, settings -> {
            Admin admin = Admin.create(settings);
            return new Connection() {
                public KafkaFuture<String> clusterId(int timeoutMs) {
                    return admin.describeCluster(new DescribeClusterOptions().timeoutMs(timeoutMs)).clusterId();
                }
                public KafkaFuture<Map<String, TopicDescription>> topics(Set<String> names, int timeoutMs) {
                    return admin.describeTopics(names, new DescribeTopicsOptions().timeoutMs(timeoutMs)).allTopicNames();
                }
                public void close(Duration timeout) { admin.close(timeout); }
            };
        });
    }

    static Expected verify(Properties properties, Plan plan, Expected expected, Function<Properties, Connection> factory) {
        Connection connection = null;
        try {
            Properties settings = new Properties();
            settings.putAll(properties);
            // Runtime-provided long timeouts cannot extend the read-only startup gate.
            for (String name : Set.of("default.api.timeout.ms", "request.timeout.ms",
                    "socket.connection.setup.timeout.ms", "socket.connection.setup.timeout.max.ms")) {
                settings.setProperty(name, Integer.toString(TIMEOUT_MS));
            }
            settings.setProperty("client.id", "streamt-identity-check");
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
            connection = factory.apply(settings);
            Set<String> names = Set.of(plan.inputTopic(), plan.outputTopic());
            var cluster = connection.clusterId(TIMEOUT_MS);
            var topics = connection.topics(names, TIMEOUT_MS);
            require(expected.clusterId().equals(cluster.get(remaining(deadline), TimeUnit.NANOSECONDS)));
            var descriptions = topics.get(remaining(deadline), TimeUnit.NANOSECONDS);
            require(descriptions != null && descriptions.keySet().equals(names));
            require(matches(descriptions.get(plan.inputTopic()), plan.inputTopic(), expected.inputTopicId()));
            require(matches(descriptions.get(plan.outputTopic()), plan.outputTopic(), expected.outputTopicId()));
            return expected;
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            // Provider exceptions may include credentials or endpoints. Keep neither cause nor message.
            throw new IllegalArgumentException("identity_verification_failed");
        } finally {
            if (connection != null) {
                try { connection.close(CLOSE_TIMEOUT); }
                catch (RuntimeException failure) { throw new IllegalArgumentException("identity_verification_failed"); }
            }
        }
    }

    private static long remaining(long deadline) {
        long remaining = deadline - System.nanoTime();
        require(remaining > 0);
        return remaining;
    }

    private static boolean matches(TopicDescription topic, String name, String id) {
        return topic != null && name.equals(topic.name()) && topic.topicId() != null
            && id.equals(topic.topicId().toString());
    }

    private static boolean canonicalTopicId(String value) {
        if (value == null || !value.matches("[A-Za-z0-9_-]{22}")) return false;
        try {
            Uuid id = Uuid.fromString(value);
            return !id.equals(Uuid.ZERO_UUID) && value.equals(id.toString());
        } catch (IllegalArgumentException invalid) { return false; }
    }

    private static void require(boolean value) {
        if (!value) throw new IllegalArgumentException("invalid_expected_or_observed_identity");
    }
}
