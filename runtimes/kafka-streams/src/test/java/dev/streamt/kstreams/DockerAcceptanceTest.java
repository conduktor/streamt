package dev.streamt.kstreams;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Uses only the uniquely owned broker/network created by acceptance.py. */
@EnabledIfEnvironmentVariable(named = "STREAMT_RUNTIME_ACCEPTANCE_IMAGE", matches = "sha256:[a-f0-9]{64}")
final class DockerAcceptanceTest {
    private final String image = System.getenv("STREAMT_RUNTIME_ACCEPTANCE_IMAGE");
    private final String token = System.getenv("STREAMT_RUNTIME_ACCEPTANCE_TOKEN");
    private final String prefix = "streamt-runtime-accept-" + token;
    private final String volume = prefix + "-state";
    private final String app = prefix + "-application";
    private final String network = System.getenv("STREAMT_RUNTIME_ACCEPTANCE_NETWORK");
    private final String bootstrap = System.getenv("STREAMT_RUNTIME_ACCEPTANCE_BOOTSTRAP");
    private final Path evidence = Path.of(System.getenv().getOrDefault("STREAMT_RUNTIME_ACCEPTANCE_EVIDENCE", "target/docker-acceptance"));
    private final List<String> containers = new ArrayList<>();
    private int generation;
    private IdentityGate.Expected identity;

    @Test @Timeout(value = 240, unit = TimeUnit.SECONDS)
    void realContainerOffsetsAndSequentialUpdates() throws Exception {
        assertNotNull(token);
        Files.createDirectories(evidence);
        Files.setPosixFilePermissions(evidence, PosixFilePermissions.fromString("rwx------"));
        String input = prefix + ".input", output = prefix + ".output";
        var plan = (com.fasterxml.jackson.databind.node.ObjectNode) Plan.JSON.readTree(PlanTest.BASE);
        plan.put("input_topic", input).put("output_topic", output);
        Path initial = evidence.resolve("plan.json"), updated = evidence.resolve("plan-updated.json");
        Files.write(initial, Plan.JSON.writeValueAsBytes(plan));
        ((com.fasterxml.jackson.databind.node.ObjectNode) plan.get("predicates").get(0)).put("value", 100);
        Files.write(updated, Plan.JSON.writeValueAsBytes(plan));
        Path properties = evidence.resolve("client.properties");
        Files.writeString(properties, "bootstrap.servers=" + System.getenv("STREAMT_RUNTIME_ACCEPTANCE_BROKER") + ":9092\nsecurity.protocol=PLAINTEXT\n");
        for (Path file : List.of(initial, updated, properties)) Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("r--r--r--"));
        var imageInfo = Plan.JSON.readTree(command("docker", "image", "inspect", image)).get(0);
        assertEquals("10001:10001", imageInfo.get("Config").get("User").textValue());
        assertEquals("0.1.1", imageInfo.get("Config").get("Labels").get("io.streamt.runner.version").textValue());
        assertTrue(!imageInfo.get("Config").hasNonNull("Volumes"), "Image must not declare anonymous volumes");
        command("docker", "volume", "create", "--label", "io.streamt.acceptance.owner=" + token, volume);
        Properties producerSettings = new Properties();
        producerSettings.put("bootstrap.servers", bootstrap);
        producerSettings.put("acks", "all");
        TopicPartition partition = new TopicPartition(input, 0);
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap));
             KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerSettings, new ByteArraySerializer(), new ByteArraySerializer())) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1), new NewTopic(output, 1, (short) 1))).all().get(15, TimeUnit.SECONDS);
            var topics = admin.describeTopics(List.of(input, output)).allTopicNames().get(10, TimeUnit.SECONDS);
            identity = new IdentityGate.Expected(admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS),
                topics.get(input).topicId().toString(), topics.get(output).topicId().toString());
            String missing = start(initial, properties, false);
            assertFailed(missing, "missing_or_invalid_offsets");
            assertEquals(-1, offset(admin, partition));
            // Explicit fixture-owned initialization. The maintained runner never chooses this offset.
            awaitEmptyGroup(admin);
            admin.alterConsumerGroupOffsets(app, Map.of(partition, new OffsetAndMetadata(0))).all().get(15, TimeUnit.SECONDS);
            send(producer, input, "a", 120L, true, new byte[] {(byte) 0xFF, 0, 1});
            send(producer, input, "b", 20L, true, PlanTest.bytes("b"));
            send(producer, input, "c", null, true, PlanTest.bytes("c"));
            send(producer, input, "d", 90L, true, PlanTest.bytes("d"));
            send(producer, input, "e", 110L, true, null);
            producer.send(new ProducerRecord<>(input, 0, PlanTest.bytes("tombstone"), null)).get(10, TimeUnit.SECONDS);
            // Valid offsets and pending records make an identity bypass observable.
            var retained = status(missing);
            for (var wrong : List.of(
                    new IdentityGate.Expected("wrong-cluster", identity.inputTopicId(), identity.outputTopicId()),
                    new IdentityGate.Expected(identity.clusterId(), "AAAAAAAAAAAAAAAAAAAAAQ", identity.outputTopicId()),
                    new IdentityGate.Expected(identity.clusterId(), identity.inputTopicId(), "AAAAAAAAAAAAAAAAAAAAAg"))) {
                String refused = start(initial, properties, false, wrong);
                assertNotEquals("0", command("docker", "wait", refused).trim());
                String logs = command("docker", "logs", refused).trim();
                assertEquals("{\"state\":\"failed\",\"reason\":\"identity_verification_failed\"}", logs);
                assertEquals(0, offset(admin, partition));
                assertEquals(List.of(), outputIds(output));
                assertEquals(retained, status(refused), "Identity failure must not overwrite retained status");
            }
            String first = start(initial, properties, false);
            awaitOffset(admin, partition, 6, first);
            closeClean(first);
            assertEquals(List.of("a", "d", "e"), outputIds(output));
            String second = start(updated, properties, false);
            send(producer, input, "f", 75L, true, PlanTest.bytes("f"));
            send(producer, input, "g", 130L, true, PlanTest.bytes("g"));
            send(producer, input, "h", 150L, false, PlanTest.bytes("h"));
            awaitOffset(admin, partition, 9, second);
            closeClean(second);
            assertEquals(List.of("a", "d", "e", "g"), outputIds(output));
            producer.send(new ProducerRecord<>(input, 0, PlanTest.bytes("bad"), PlanTest.bytes("{\"id\":\"SUPER_SECRET_EVENT\",\"amount\":\"bad\",\"active\":true}"))).get(10, TimeUnit.SECONDS);
            String poison = start(updated, properties, false);
            assertFailed(poison, "processing_failed");
            assertEquals(9, offset(admin, partition));
            assertFalse(command("docker", "logs", poison).contains("SUPER_SECRET_EVENT"));
            // Simulate invalid retained progress only in this owned fixture; no fallback/reset is allowed.
            awaitEmptyGroup(admin);
            admin.alterConsumerGroupOffsets(app, Map.of(partition, new OffsetAndMetadata(1000))).all().get(15, TimeUnit.SECONDS);
            String lost = start(updated, properties, false);
            assertFailed(lost, "missing_or_invalid_offsets");
            assertEquals(1000, offset(admin, partition));
            assertEquals(List.of("a", "d", "e", "g"), outputIds(output));
            String validation = start(initial, properties, true);
            assertEquals("0", command("docker", "wait", validation).trim());
            assertTrue(command("docker", "logs", validation).contains("\"state\":\"validated\""));
            var result = Plan.JSON.createObjectNode().put("image_id", image).put("application_id", app)
                .put("container_generations", generation).put("default_offsets_rejected", true)
                .put("initialization", "explicit fixture Admin.alterConsumerGroupOffsets(0)")
                .put("initial_committed_offset", 6).put("updated_committed_offset", 9)
                .put("poison_committed_offset", 9).put("invalid_committed_offset_unchanged", 1000)
                .put("network_none_validation_passed", true).put("secret_record_absent_from_logs", true)
                .put("identity_mismatch_checks", 3).put("identity_mismatch_committed_offset_unchanged", 0)
                .put("cluster_id", identity.clusterId()).put("input_topic_id", identity.inputTopicId())
                .put("output_topic_id", identity.outputTopicId())
                .put("same_named_state_volume", volume).put("clean_stop_checks", 2);
            var ids = result.putArray("output_ids");
            for (String id : List.of("a", "d", "e", "g")) ids.add(id);
            Files.writeString(evidence.resolve("result.json"), Plan.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
            System.out.println("DOCKER_ACCEPTANCE_PASSED " + result);
        } finally {
            for (String container : containers) {
                Files.writeString(evidence.resolve(container + ".log"), command("docker", "logs", container));
                assertEquals(token, command("docker", "inspect", container, "--format", "{{ index .Config.Labels \"io.streamt.acceptance.owner\" }}").trim());
                command("docker", "rm", "-f", "-v", container);
            }
            assertEquals(token, command("docker", "volume", "inspect", volume, "--format", "{{ index .Labels \"io.streamt.acceptance.owner\" }}").trim());
            command("docker", "volume", "rm", volume);
        }
    }

    private String start(Path plan, Path properties, boolean validate) throws Exception {
        return start(plan, properties, validate, identity);
    }

    private String start(Path plan, Path properties, boolean validate, IdentityGate.Expected expected) throws Exception {
        String name = prefix + "-runner-" + (++generation);
        List<String> args = new ArrayList<>(List.of("docker", "create", "--pull=never", "--name", name,
            "--label", "io.streamt.acceptance.owner=" + token, "--network", validate ? "none" : network,
            "--restart=no", "--read-only", "--tmpfs", "/tmp:rw,nosuid,noexec,size=64m", "--cap-drop=ALL",
            "--security-opt", "no-new-privileges", "--memory", "512m",
            "--mount", "type=bind,source=" + plan.toAbsolutePath() + ",target=/run/streamt/plan.json,readonly",
            "--mount", "type=bind,source=" + properties.toAbsolutePath() + ",target=/run/streamt/client.properties,readonly",
            "--mount", "type=volume,source=" + volume + ",target=/var/lib/streamt/state", image,
            "--plan", "/run/streamt/plan.json", "--client-properties", "/run/streamt/client.properties",
            "--application-id", app, "--state-dir", "/var/lib/streamt/state",
            "--expected-cluster-id", expected.clusterId(), "--expected-input-topic-id", expected.inputTopicId(),
            "--expected-output-topic-id", expected.outputTopicId()));
        if (validate) args.add("--validate-only");
        command(args.toArray(String[]::new));
        containers.add(name);
        command("docker", "start", name);
        return name;
    }

    private void closeClean(String name) throws Exception {
        assertEquals("true", command("docker", "inspect", name, "--format", "{{.State.Running}}").trim());
        command("docker", "stop", "--timeout", "20", name);
        var status = status(name);
        assertEquals("closed", status.get("state").textValue());
        assertTrue(status.get("reason").isNull());
        assertEquals(app, status.get("application_id").textValue());
        assertEquals(identity.clusterId(), status.get("cluster_id").textValue());
        assertEquals(identity.inputTopicId(), status.get("input_topic_id").textValue());
        assertEquals(identity.outputTopicId(), status.get("output_topic_id").textValue());
        assertEquals("no", command("docker", "inspect", name, "--format", "{{.HostConfig.RestartPolicy.Name}}").trim());
        assertEquals("0", command("docker", "inspect", name, "--format", "{{.RestartCount}}").trim());
    }

    private void assertFailed(String name, String reason) throws Exception {
        assertNotEquals("0", command("docker", "wait", name).trim());
        var status = status(name);
        assertEquals("failed", status.get("state").textValue());
        assertEquals(reason, status.get("reason").textValue());
    }

    private com.fasterxml.jackson.databind.JsonNode status(String name) throws Exception {
        Path destination = evidence.resolve(name + "-status.json");
        command("docker", "cp", name + ":/var/lib/streamt/state/status.json", destination.toString());
        return Plan.JSON.readTree(Files.readAllBytes(destination));
    }

    private long offset(Admin admin, TopicPartition partition) throws Exception {
        var values = admin.listConsumerGroupOffsets(app).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        return values.containsKey(partition) ? values.get(partition).offset() : -1;
    }

    private void awaitEmptyGroup(Admin admin) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            var group = admin.describeConsumerGroups(List.of(app)).describedGroups().get(app).get(10, TimeUnit.SECONDS);
            if (group.members().isEmpty()) return;
            Thread.sleep(200);
        }
        fail("Exited runner still has group members; offset changes remain blocked");
    }

    private void awaitOffset(Admin admin, TopicPartition partition, long expected, String container) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(35).toNanos();
        while (System.nanoTime() < deadline) {
            assertEquals("true", command("docker", "inspect", container, "--format", "{{.State.Running}}").trim());
            long actual = offset(admin, partition);
            if (actual == expected) return;
            assertTrue(actual < expected);
            Thread.sleep(100);
        }
        fail("Timed out waiting for committed offset " + expected);
    }

    private List<String> outputIds(String topic) {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(Map.of("bootstrap.servers", bootstrap,
                "isolation.level", "read_committed", "enable.auto.commit", "false"), new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition)); consumer.seek(partition, 0);
            long end = consumer.endOffsets(List.of(partition), Duration.ofSeconds(10)).get(partition);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            List<String> ids = new ArrayList<>();
            while (consumer.position(partition) < end && System.nanoTime() < deadline) {
                for (var row : consumer.poll(Duration.ofMillis(100))) {
                    try {
                        var json = Plan.JSON.readTree(row.value());
                        String id = json.get("order_id").textValue();
                        if (id.equals("a")) assertArrayEquals(new byte[] {(byte) 0xFF, 0, 1}, row.key());
                        if (id.equals("e")) assertNull(row.key());
                        ids.add(id);
                    } catch (IOException invalid) { throw new AssertionError(invalid); }
                }
            }
            assertTrue(consumer.position(partition) >= end);
            return ids;
        }
    }

    private void send(KafkaProducer<byte[], byte[]> producer, String topic, String id, Long amount, boolean active, byte[] key) throws Exception {
        var body = Plan.JSON.createObjectNode().put("id", id).put("active", active);
        if (amount == null) body.putNull("amount"); else body.put("amount", amount);
        producer.send(new ProducerRecord<>(topic, 0, key, Plan.JSON.writeValueAsBytes(body))).get(10, TimeUnit.SECONDS);
    }

    private static String command(String... args) throws Exception {
        Process process = new ProcessBuilder(args).redirectErrorStream(true).start();
        // Docker/Maven command output here is bounded metadata, not a continuous log stream.
        var bytes = new java.io.ByteArrayOutputStream();
        Thread reader = new Thread(() -> {
            try { process.getInputStream().transferTo(bytes); } catch (IOException ignored) { }
        });
        reader.start();
        if (!process.waitFor(50, TimeUnit.SECONDS)) {
            process.destroyForcibly(); reader.join(1000);
            fail("Scoped acceptance command timed out: " + args[0]);
        }
        reader.join(1000);
        assertEquals(0, process.exitValue(), bytes.toString(java.nio.charset.StandardCharsets.UTF_8));
        return bytes.toString(java.nio.charset.StandardCharsets.UTF_8);
    }
}
