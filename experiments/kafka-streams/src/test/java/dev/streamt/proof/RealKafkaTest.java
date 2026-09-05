package dev.streamt.proof;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Only run with the disposable broker created by run_proof.py. */
@EnabledIfEnvironmentVariable(named = "STREAMT_PROOF_BOOTSTRAP", matches = ".+")
final class RealKafkaTest {
    private final String bootstrap = System.getenv("STREAMT_PROOF_BOOTSTRAP");
    private final String app = System.getenv("STREAMT_PROOF_APPLICATION_ID");
    private final Path evidence = Path.of(System.getenv().getOrDefault("STREAMT_PROOF_EVIDENCE", "target/real-proof"));
    private Process running;
    private int generation;

    @Test @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void realRecordsChangedPredicateAndStableOffsets() throws Exception {
        assertEquals("4.3.1", AppInfoParser.getVersion());
        Files.createDirectories(evidence);
        Plan plan = new Plan(Files.readAllBytes(evidence.resolve("plan.json")));
        TopicPartition source = new TopicPartition(plan.inputTopic(), 0);
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", bootstrap);
        producerConfig.put("acks", "all");
        producerConfig.put("delivery.timeout.ms", 30000);
        producerConfig.put("request.timeout.ms", 10000);
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap));
             KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig, new ByteArraySerializer(), new ByteArraySerializer())) {
            // Source is fixture-owned; the runner itself never creates topics.
            admin.createTopics(List.of(new NewTopic(plan.inputTopic(), 1, (short) 1),
                new NewTopic(plan.outputTopic(), 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            start("plan.json");
            send(producer, plan.inputTopic(), "a", 120L, true, new byte[] {(byte) 0xFF, 0, 1});
            send(producer, plan.inputTopic(), "b", 20L, true, PlanTest.bytes("b"));
            send(producer, plan.inputTopic(), "c", null, true, PlanTest.bytes("c"));
            send(producer, plan.inputTopic(), "d", 90L, true, PlanTest.bytes("d"));
            send(producer, plan.inputTopic(), "e", 110L, true, null);
            producer.send(new ProducerRecord<>(plan.inputTopic(), 0, PlanTest.bytes("tombstone"), null)).get(10, TimeUnit.SECONDS);
            awaitOffset(admin, source, 6);
            stopClean();
            List<ConsumerRecord<byte[], byte[]>> first = readAll(plan.outputTopic());
            assertIds(first, "a", "d", "e");
            assertArrayEquals(new byte[] {(byte) 0xFF, 0, 1}, first.get(0).key());
            assertNull(first.get(2).key());

            // Same application.id, source/output, state directory. Only predicate 50 -> 100 changes.
            start("plan-updated.json");
            send(producer, plan.inputTopic(), "f", 75L, true, PlanTest.bytes("f"));
            send(producer, plan.inputTopic(), "g", 130L, true, PlanTest.bytes("g"));
            send(producer, plan.inputTopic(), "h", 150L, false, PlanTest.bytes("h"));
            awaitOffset(admin, source, 9);
            stopClean();
            List<ConsumerRecord<byte[], byte[]>> after = readAll(plan.outputTopic());
            assertIds(after, "a", "d", "e", "g");
            assertEquals(130, Plan.JSON.readTree(after.get(3).value()).get("amount").longValue());

            // A malformed typed record terminates the process and does not advance its source offset.
            producer.send(new ProducerRecord<>(plan.inputTopic(), 0, PlanTest.bytes("poison"),
                PlanTest.bytes("{\"id\":\"poison\",\"amount\":\"wrong-type\",\"active\":true}"))).get(10, TimeUnit.SECONDS);
            start("plan-updated.json");
            assertTrue(running.waitFor(40, TimeUnit.SECONDS), "Poison record did not stop runner");
            assertNotEquals(0, running.exitValue());
            assertTrue(Files.readString(evidence.resolve("runner-3.log")).contains("PROOF_PROCESSING_FAILED"));
            assertEquals(9, offset(admin, source));
            assertIds(readAll(plan.outputTopic()), "a", "d", "e", "g");
            var report = Plan.JSON.createObjectNode();
            report.put("kafka_client_version", AppInfoParser.getVersion());
            report.put("application_id", app);
            report.put("input_topic", plan.inputTopic());
            report.put("output_topic", plan.outputTopic());
            report.put("processing_setting", "exactly_once_v2");
            report.put("consumer_isolation", "read_committed");
            report.put("initial_committed_offset", 6);
            report.put("updated_committed_offset", 9);
            report.put("poison_committed_offset", 9);
            report.put("process_starts", generation);
            report.put("binary_key_preserved", true);
            report.put("null_key_preserved", true);
            report.put("tombstone_dropped", true);
            report.put("update", "clean shutdown; same application.id, input/output topics and state directory; filter >=50 to >=100");
            report.put("not_proven", "crash recovery, rebalances, broker failure, stateful topology updates, production lifecycle safety");
            var rows = report.putArray("output_rows");
            for (var record : after) rows.add(Plan.JSON.readTree(record.value()));
            Files.writeString(evidence.resolve("result.json"), Plan.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report));
            System.out.println("REAL_KAFKA_PROOF_PASSED " + report);
        } finally {
            cleanup();
        }
    }

    private void start(String plan) throws Exception {
        generation++;
        running = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-jar", "target/kafka-streams-proof-0.0.0-experiment.jar",
            evidence.resolve(plan).toString(), bootstrap, app, evidence.resolve("state").toString())
            .redirectErrorStream(true).redirectOutput(evidence.resolve("runner-" + generation + ".log").toFile()).start();
    }

    private void stopClean() throws Exception {
        requireCleanStop(running, evidence.resolve("runner-" + generation + ".log"));
        running = null;
    }

    static void requireCleanStop(Process process, Path log) throws Exception {
        assertNotNull(process, "No running process to shut down");
        assertTrue(process.isAlive(), "Runner exited before the required clean shutdown");
        process.destroy();
        assertTrue(process.waitFor(20, TimeUnit.SECONDS), "Runner did not exit within clean-close bound");
        String output = Files.readString(log);
        assertTrue(output.contains("PROOF_CLOSED"), "Runner did not confirm bounded close");
        assertFalse(output.contains("PROOF_CLOSE_TIMEOUT"), "Runner close timed out");
        assertFalse(output.contains("PROOF_PROCESSING_FAILED"), "Runner failed before clean shutdown");
    }

    private void cleanup() throws Exception {
        if (running != null && running.isAlive()) {
            running.destroy();
            if (!running.waitFor(20, TimeUnit.SECONDS)) {
                running.destroyForcibly();
                running.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private void send(KafkaProducer<byte[], byte[]> producer, String topic, String id, Long amount, boolean active, byte[] key) throws Exception {
        var row = Plan.JSON.createObjectNode().put("id", id).put("active", active);
        if (amount == null) row.putNull("amount"); else row.put("amount", amount);
        producer.send(new ProducerRecord<>(topic, 0, key, Plan.JSON.writeValueAsBytes(row))).get(10, TimeUnit.SECONDS);
    }

    private long offset(Admin admin, TopicPartition source) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(app).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        return offsets.containsKey(source) ? offsets.get(source).offset() : -1;
    }

    private void awaitOffset(Admin admin, TopicPartition source, long wanted) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
        while (System.nanoTime() < deadline) {
            assertTrue(running.isAlive(), "Runner exited; inspect " + evidence.resolve("runner-" + generation + ".log"));
            long current = offset(admin, source);
            if (current == wanted) return;
            assertTrue(current < wanted, "Unexpected source offset");
            Thread.sleep(100);
        }
        fail("Timed out waiting for committed offset " + wanted);
    }

    private List<ConsumerRecord<byte[], byte[]>> readAll(String topic) {
        Properties config = new Properties();
        config.put("bootstrap.servers", bootstrap);
        config.put("enable.auto.commit", false);
        config.put("isolation.level", "read_committed");
        config.put("allow.auto.create.topics", false);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seek(partition, 0);
            long end = consumer.endOffsets(List.of(partition), Duration.ofSeconds(10)).get(partition);
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            List<ConsumerRecord<byte[], byte[]>> rows = new ArrayList<>();
            while (consumer.position(partition) < end && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(100)).forEach(rows::add);
            }
            assertTrue(consumer.position(partition) >= end, "Output snapshot incomplete");
            return rows;
        }
    }

    private void assertIds(List<ConsumerRecord<byte[], byte[]>> records, String... ids) throws Exception {
        List<String> actual = new ArrayList<>();
        for (var record : records) actual.add(Plan.JSON.readTree(record.value()).get("order_id").textValue());
        assertEquals(List.of(ids), actual);
    }
}
