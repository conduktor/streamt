package dev.streamt.kstreams;

import static org.junit.jupiter.api.Assertions.*;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class RunnerTest {
    static final IdentityGate.Expected IDENTITY = new IdentityGate.Expected("cluster-unit",
        "AAAAAAAAAAAAAAAAAAAAAQ", "AAAAAAAAAAAAAAAAAAAAAg");

    private static String[] args(Path plan, Path properties, Path state, boolean validate) {
        var values = new java.util.ArrayList<>(java.util.List.of("--plan", plan.toString(),
            "--client-properties", properties.toString(), "--application-id", "test", "--state-dir", state.toString(),
            "--expected-cluster-id", IDENTITY.clusterId(), "--expected-input-topic-id", IDENTITY.inputTopicId(),
            "--expected-output-topic-id", IDENTITY.outputTopicId()));
        if (validate) values.add("--validate-only");
        return values.toArray(String[]::new);
    }

    @Test void deterministicVersionOutput() {
        var output = capture(() -> assertEquals(0, Runner.execute(new String[] {"--version"})));
        assertEquals("{\"runner_version\":\"0.1.1\",\"plan_version\":1,\"kafka_version\":\"4.3.1\"}\n", output);
    }

    @Test void validateOnlyDoesNotCreateClientOrState(@TempDir Path directory) throws Exception {
        Path plan = directory.resolve("plan.json"), properties = directory.resolve("client.properties"), state = directory.resolve("state");
        Files.writeString(plan, PlanTest.BASE);
        Files.writeString(properties, "bootstrap.servers=not-resolvable.invalid:9092\n");
        var output = capture(() -> assertEquals(0, Runner.execute(args(plan, properties, state, true),
            (connection, compiled, expected) -> { throw new AssertionError("validate-only constructed an identity client"); })));
        assertTrue(output.contains("\"state\":\"validated\""));
        assertFalse(Files.exists(state));
        assertFalse(output.contains("not-resolvable"));
    }

    @Test void invalidPropertiesNeverPrintSecrets(@TempDir Path directory) throws Exception {
        Path plan = directory.resolve("plan.json"), properties = directory.resolve("secret.properties");
        Files.writeString(plan, PlanTest.BASE);
        Files.writeString(properties, "bootstrap.servers=broker:9092\nsecurity.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=SUPER_SECRET_BROKEN_JAAS\n");
        var output = capture(() -> assertEquals(2, Runner.execute(args(plan, properties, directory, true))));
        assertEquals("{\"state\":\"failed\",\"reason\":\"client_properties_invalid\"}\n", output);
    }

    @ParameterizedTest @ValueSource(strings = {"--unknown", "--application-id", "--plan"})
    void rejectsInvalidArguments(String option) {
        assertEquals(2, Runner.execute(new String[] {option}));
    }

    @ParameterizedTest @ValueSource(strings = {"--expected-cluster-id", "--expected-input-topic-id", "--expected-output-topic-id"})
    void identitiesAreMandatoryEvenOffline(String option, @TempDir Path directory) {
        var values = new java.util.ArrayList<>(java.util.List.of(args(directory, directory, directory, true)));
        int index = values.indexOf(option);
        values.remove(index); values.remove(index);
        assertThrows(IllegalArgumentException.class, () -> Runner.arguments(values.toArray(String[]::new)));
    }

    @Test void kafkaBase64UrlIdentityMayBeginWithTwoDashes(@TempDir Path directory) {
        String[] values = args(directory, directory, directory, true);
        for (int index = 0; index < values.length; index++) {
            if (values[index].equals("--expected-input-topic-id")) values[index + 1] = "--AAAAAAAAAAAAAAAAAAAQ";
        }
        assertEquals("--AAAAAAAAAAAAAAAAAAAQ", Runner.arguments(values).expected().inputTopicId());
    }

    @Test void identityFailurePreventsStateAndStreamsConstruction(@TempDir Path directory) throws Exception {
        Path plan = directory.resolve("plan.json"), properties = directory.resolve("client.properties"), state = directory.resolve("state");
        Files.writeString(plan, PlanTest.BASE);
        Files.writeString(properties, "bootstrap.servers=unresolvable.invalid:9092\n");
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        String output = capture(() -> assertEquals(2, Runner.execute(args(plan, properties, state, false),
            (connection, compiled, expected) -> {
                calls.incrementAndGet();
                assertEquals(IDENTITY, expected);
                throw new IllegalStateException("SUPER_SECRET_PROVIDER_DETAIL");
            })));
        assertEquals(1, calls.get());
        assertFalse(Files.exists(state));
        assertEquals("{\"state\":\"failed\",\"reason\":\"identity_verification_failed\"}\n", output);
    }

    @Test void recognizesMissingAndLostOffsetsThroughWrappedExceptions() {
        var partition = new TopicPartition("input", 0);
        assertEquals("missing_or_invalid_offsets", Runner.failureReason(new RuntimeException(new NoOffsetForPartitionException(partition))));
        assertEquals("missing_or_invalid_offsets", Runner.failureReason(new OffsetOutOfRangeException(Map.of(partition, 15L))));
        assertEquals("processing_failed", Runner.failureReason(new IllegalArgumentException("secret record")));
    }

    @Test void statusIsAtomicAndFailedEvidenceSurvivesClose(@TempDir Path directory) throws Exception {
        var status = new StatusFile(directory, "orders", "sha256:test", IDENTITY);
        capture(() -> {
            try {
                status.transition("starting", null);
                status.transition("running", null);
                status.transition("failed", "missing_or_invalid_offsets");
                status.transition("closed", null);
            } catch (Exception error) { throw new AssertionError(error); }
        });
        var body = Plan.JSON.readTree(Files.readAllBytes(directory.resolve("status.json")));
        assertEquals("failed", body.get("state").textValue());
        assertEquals("missing_or_invalid_offsets", body.get("reason").textValue());
        assertEquals("orders", body.get("application_id").textValue());
        assertEquals(IDENTITY.clusterId(), body.get("cluster_id").textValue());
        assertEquals(IDENTITY.inputTopicId(), body.get("input_topic_id").textValue());
        assertEquals(IDENTITY.outputTopicId(), body.get("output_topic_id").textValue());
        assertEquals(10, body.size());
        try (var files = Files.list(directory)) { assertEquals(1, files.count()); }
    }

    static String capture(Runnable action) {
        PrintStream oldOut = System.out, oldErr = System.err;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (PrintStream output = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            System.setOut(output); System.setErr(output);
            action.run();
        } finally { System.setOut(oldOut); System.setErr(oldErr); }
        return bytes.toString(StandardCharsets.UTF_8);
    }
}
