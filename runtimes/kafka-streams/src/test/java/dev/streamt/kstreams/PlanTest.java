package dev.streamt.kstreams;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class PlanTest {
    static final String BASE = """
        {"version":1,"input_topic":"proof.input","output_topic":"proof.output",
         "schema":{"id":{"type":"STRING","nullable":false},"amount":{"type":"BIGINT","nullable":true},"active":{"type":"BOOLEAN","nullable":false}},
         "projection":[{"column":"id","as":"order_id"},{"column":"amount","as":"amount"}],
         "predicates":[{"column":"amount","op":"ge","value":50},{"column":"active","op":"eq","value":true}]}
        """;
    static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }
    static Plan plan() { return new Plan(bytes(BASE)); }

    @Test void projectsAndFiltersWithIntegerEqualityAcrossJsonRepresentations() throws Exception {
        assertEquals(Plan.JSON.readTree("{\"order_id\":\"a\",\"amount\":120}"),
            Plan.JSON.readTree(plan().transform(bytes("{\"id\":\"a\",\"amount\":120,\"active\":true}"))));
        assertNull(plan().transform(bytes("{\"id\":\"b\",\"amount\":20,\"active\":true}")));
        assertNull(plan().transform(bytes("{\"id\":\"c\",\"amount\":null,\"active\":true}")));
        assertNull(plan().transform(bytes("{\"id\":\"d\",\"amount\":120,\"active\":false}")));
        assertNull(plan().transform(null));
    }

    @ParameterizedTest @ValueSource(strings = {
        "{}", "[]", "null", "", "{bad}",
        "{\"id\":\"a\",\"amount\":\"120\",\"active\":true}",
        "{\"id\":\"a\",\"amount\":120.0,\"active\":true}",
        "{\"id\":\"a\",\"amount\":9223372036854775808,\"active\":true}",
        "{\"id\":null,\"amount\":120,\"active\":true}",
        "{\"id\":\"a\",\"amount\":120,\"active\":\"true\"}",
        "{\"id\":\"a\",\"amount\":120,\"active\":true,\"extra\":1}",
        "{\"id\":\"a\",\"id\":\"b\",\"amount\":120,\"active\":true}",
        "{\"id\":\"a\",\"amount\":120,\"active\":true} {}"
    }) void rejectsBadRecords(String record) {
        assertThrows(IllegalArgumentException.class, () -> plan().transform(bytes(record)));
    }

    @Test void rejectsInvalidUtf8() {
        assertThrows(IllegalArgumentException.class, () -> plan().transform(new byte[] {(byte) 0xC3, 0x28}));
    }

    @Test void runnerRejectsPlanBeforeReadingInvalidBrokerConfiguration(@TempDir Path directory) throws Exception {
        Path path = directory.resolve("invalid-plan.json");
        Files.writeString(path, BASE.replace("\"ge\"", "\"run_arbitrary_code\""));
        String output = RunnerTest.capture(() -> assertEquals(2, Runner.execute(new String[] {
            "--plan", path.toString(), "--client-properties", directory.resolve("missing.properties").toString(),
            "--application-id", "proof", "--state-dir", directory.toString(),
            "--expected-cluster-id", RunnerTest.IDENTITY.clusterId(),
            "--expected-input-topic-id", RunnerTest.IDENTITY.inputTopicId(),
            "--expected-output-topic-id", RunnerTest.IDENTITY.outputTopicId(), "--validate-only"})));
        assertEquals("{\"state\":\"failed\",\"reason\":\"plan_invalid\"}\n", output);
    }

    @ParameterizedTest @ValueSource(strings = {"gt", "ge", "lt", "le", "eq", "ne"})
    void numericOperatorsHaveCorrectEqualityBoundary(String op) throws Exception {
        ObjectNode root = (ObjectNode) Plan.JSON.readTree(BASE);
        root.putArray("predicates").addObject().put("column", "amount").put("op", op).put("value", 50);
        Plan plan = new Plan(Plan.JSON.writeValueAsBytes(root));
        byte[] result = plan.transform(bytes("{\"id\":\"a\",\"amount\":50,\"active\":true}"));
        assertEquals(java.util.Set.of("eq", "ge", "le").contains(op), result != null);
    }

    @Test void stringEqualityIsExactAndBigIntRangeIsAccepted() throws Exception {
        ObjectNode root = (ObjectNode) Plan.JSON.readTree(BASE);
        root.putArray("predicates").addObject().put("column", "id").put("op", "eq").put("value", "été");
        Plan plan = new Plan(Plan.JSON.writeValueAsBytes(root));
        assertNotNull(plan.transform(bytes("{\"id\":\"été\",\"amount\":9223372036854775807,\"active\":true}")));
        assertNull(plan.transform(bytes("{\"id\":\"Été\",\"amount\":-9223372036854775808,\"active\":true}")));
    }

    @ParameterizedTest @ValueSource(strings = {"gt", "ge", "lt", "le", "eq", "ne", "is_null", "not_null"})
    void predicatesHaveExplicitNullSemantics(String op) throws Exception {
        ObjectNode root = (ObjectNode) Plan.JSON.readTree(BASE);
        var predicate = root.putArray("predicates").addObject().put("column", "amount").put("op", op);
        if (!op.endsWith("null")) predicate.put("value", 50);
        byte[] result = new Plan(Plan.JSON.writeValueAsBytes(root)).transform(bytes("{\"id\":\"a\",\"amount\":null,\"active\":true}"));
        if (op.equals("is_null")) assertNotNull(result); else assertNull(result);
    }

    @Test void rejectsTamperedPlans() throws Exception {
        for (String bad : new String[] {
            BASE.replace("\"version\":1", "\"version\":2"),
            BASE.replace("\"version\":1", "\"version\":4294967297"),
            BASE.replace("\"version\":1", "\"version\":1,\"unknown\":true"),
            BASE.replace("\"version\":1", "\"version\":1,\"version\":1"),
            BASE.replace("\"ge\"", "\"execute\""),
            BASE.replace("\"BIGINT\"", "\"DECIMAL\""),
            BASE.replace("\"value\":50", "\"value\":\"50\""),
            BASE.replace("\"value\":50", "\"value\":null"),
            BASE.replace("\"column\":\"amount\",\"op\"", "\"column\":\"unknown\",\"op\""),
            BASE.replace("proof.output", "proof.input"),
            BASE.replace("\"order_id\"", "\"amount\""),
            BASE + " {}"
        }) assertThrows(IllegalArgumentException.class, () -> new Plan(bytes(bad)), bad);
    }

    @Test void topologyPreservesBinaryAndNullKeys() {
        Properties connection = new Properties();
        connection.put("bootstrap.servers", "localhost:1");
        Properties config = ClientProperties.streams(connection, "offline-proof", Path.of("target", "unit-state").toAbsolutePath());
        try (TopologyTestDriver driver = new TopologyTestDriver(Runner.topology(plan()), config)) {
            var input = driver.createInputTopic("proof.input", new ByteArraySerializer(), new ByteArraySerializer());
            var output = driver.createOutputTopic("proof.output", new ByteArrayDeserializer(), new ByteArrayDeserializer());
            byte[] key = {(byte) 0xFF, 0, 1};
            byte[] value = bytes("{\"id\":\"a\",\"amount\":120,\"active\":true}");
            input.pipeInput(key, value);
            input.pipeInput(null, value);
            input.pipeInput(key, (byte[]) null);
            assertArrayEquals(key, output.readKeyValue().key);
            assertNull(output.readKeyValue().key);
            assertTrue(output.isEmpty());
        }
    }
}
