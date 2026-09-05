package dev.streamt.kstreams;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

final class IdentityGateTest {
    private static final IdentityGate.Expected EXPECTED = RunnerTest.IDENTITY;
    private static TopicDescription topic(String name, String id) {
        return new TopicDescription(name, false, List.of(), Set.of(), Uuid.fromString(id));
    }

    private static final class Connection implements IdentityGate.Connection {
        KafkaFuture<String> cluster = KafkaFuture.completedFuture(EXPECTED.clusterId());
        Map<String, TopicDescription> descriptions = new HashMap<>(Map.of(
            "input", topic("input", EXPECTED.inputTopicId()), "output", topic("output", EXPECTED.outputTopicId())));
        int closed;
        boolean closeFails;
        public KafkaFuture<String> clusterId(int timeoutMs) {
            assertEquals(10_000, timeoutMs);
            return cluster;
        }
        public KafkaFuture<Map<String, TopicDescription>> topics(Set<String> names, int timeoutMs) {
            assertEquals(Set.of("input", "output"), names);
            assertEquals(10_000, timeoutMs);
            return KafkaFuture.completedFuture(descriptions);
        }
        public void close(Duration timeout) {
            assertEquals(Duration.ofSeconds(2), timeout);
            closed++;
            if (closeFails) throw new IllegalStateException("SUPER_SECRET_CLOSE");
        }
    }

    private static Plan plan() {
        return new Plan(PlanTest.bytes(PlanTest.BASE.replace("proof.input", "input").replace("proof.output", "output")));
    }

    @Test void checksExactBindingsAndClosesReadOnlyClient() throws Exception {
        var connection = new Connection();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "broker:9092");
        properties.setProperty("request.timeout.ms", "300000");
        properties.setProperty("sasl.jaas.config", "SUPER_SECRET_JAAS");
        var result = IdentityGate.verify(properties, plan(), EXPECTED, settings -> {
            for (String name : Set.of("default.api.timeout.ms", "request.timeout.ms", "socket.connection.setup.timeout.ms",
                    "socket.connection.setup.timeout.max.ms")) assertEquals("10000", settings.getProperty(name));
            assertEquals("SUPER_SECRET_JAAS", settings.getProperty("sasl.jaas.config"));
            assertFalse(settings.containsKey("application.id"));
            return connection;
        });
        assertEquals(EXPECTED, result);
        assertEquals(1, connection.closed);
        assertEquals("300000", properties.getProperty("request.timeout.ms"));
    }

    @ParameterizedTest @ValueSource(strings = {"cluster", "input", "output", "missing", "extra", "wrong-name", "zero", "null"})
    void refusesUnknownOrDifferentObservedIdentities(String mismatch) throws Exception {
        var connection = new Connection();
        switch (mismatch) {
            case "cluster" -> connection.cluster = KafkaFuture.completedFuture("other-cluster");
            case "input", "output" -> connection.descriptions.put(mismatch, topic(mismatch, "AAAAAAAAAAAAAAAAAAAAAw"));
            case "missing" -> connection.descriptions.remove("input");
            case "extra" -> connection.descriptions.put("extra", topic("extra", "AAAAAAAAAAAAAAAAAAAAAw"));
            case "wrong-name" -> connection.descriptions.put("input", topic("different", EXPECTED.inputTopicId()));
            case "zero" -> connection.descriptions.put("input", topic("input", Uuid.ZERO_UUID.toString()));
            case "null" -> connection.descriptions.put("input", null);
        }
        var failure = assertThrows(IllegalArgumentException.class,
            () -> IdentityGate.verify(new Properties(), plan(), EXPECTED, settings -> connection));
        assertEquals("identity_verification_failed", failure.getMessage());
        assertNull(failure.getCause());
        assertEquals(1, connection.closed);
    }

    @ParameterizedTest @NullAndEmptySource
    @ValueSource(strings = {"AAAAAAAAAAAAAAAAAAAAAA", "AAAAAAAAAAAAAAAAAAAAAR", "AAAAAAAAAAAAAAAAAAAAAQ=", "bad", "secret\nvalue"})
    void rejectsInvalidNoncanonicalOrZeroTopicIds(String id) {
        assertThrows(IllegalArgumentException.class, () -> new IdentityGate.Expected("cluster", id, EXPECTED.outputTopicId()));
        assertThrows(IllegalArgumentException.class, () -> new IdentityGate.Expected("cluster", EXPECTED.inputTopicId(), id));
    }

    @ParameterizedTest @NullAndEmptySource @ValueSource(strings = {"two clusters", "cluster/secret", "\nsecret"})
    void rejectsInvalidClusterIds(String id) {
        assertThrows(IllegalArgumentException.class, () -> new IdentityGate.Expected(id, EXPECTED.inputTopicId(), EXPECTED.outputTopicId()));
    }

    @Test void rejectsSameTopicIdentityAndExcessiveClusterLength() {
        assertThrows(IllegalArgumentException.class, () -> new IdentityGate.Expected("cluster", EXPECTED.inputTopicId(), EXPECTED.inputTopicId()));
        assertThrows(IllegalArgumentException.class, () -> new IdentityGate.Expected("c".repeat(201), EXPECTED.inputTopicId(), EXPECTED.outputTopicId()));
    }

    @Test void timeoutUsesSharedBoundAndAlwaysCloses() throws Exception {
        var connection = new Connection();
        var waits = new AtomicInteger();
        connection.cluster = new KafkaFutureImpl<>() {
            @Override public String get(long timeout, TimeUnit unit) throws TimeoutException {
                assertEquals(TimeUnit.NANOSECONDS, unit);
                assertTrue(timeout > 0 && timeout <= Duration.ofSeconds(10).toNanos());
                waits.incrementAndGet();
                throw new TimeoutException("SUPER_SECRET_TIMEOUT");
            }
        };
        var failure = assertThrows(IllegalArgumentException.class,
            () -> IdentityGate.verify(new Properties(), plan(), EXPECTED, settings -> connection));
        assertEquals("identity_verification_failed", failure.getMessage());
        assertNull(failure.getCause());
        assertEquals(1, waits.get());
        assertEquals(1, connection.closed);
    }

    @Test void providerAndCloseErrorsAreSecretNeutral() throws Exception {
        var failure = assertThrows(IllegalArgumentException.class,
            () -> IdentityGate.verify(new Properties(), plan(), EXPECTED,
                settings -> { throw new IllegalArgumentException("SUPER_SECRET_PROVIDER"); }));
        assertEquals("identity_verification_failed", failure.getMessage());
        assertNull(failure.getCause());
        var connection = new Connection();
        connection.closeFails = true;
        failure = assertThrows(IllegalArgumentException.class,
            () -> IdentityGate.verify(new Properties(), plan(), EXPECTED, settings -> connection));
        assertEquals("identity_verification_failed", failure.getMessage());
        assertNull(failure.getCause());
        assertEquals(1, connection.closed);
    }

    @Test void interruptedWaitRestoresInterruptAndCloses() {
        var connection = new Connection();
        connection.cluster = new KafkaFutureImpl<>() {
            @Override public String get(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException("SUPER_SECRET_INTERRUPT");
            }
        };
        try {
            var failure = assertThrows(IllegalArgumentException.class,
                () -> IdentityGate.verify(new Properties(), plan(), EXPECTED, settings -> connection));
            assertEquals("identity_verification_failed", failure.getMessage());
            assertNull(failure.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, connection.closed);
        } finally { Thread.interrupted(); }
    }
}
