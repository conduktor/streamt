package dev.streamt.proof;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

/** Experiment entry point, intentionally outside streamt's deployers. */
public final class Runner {
    public static Topology topology(Plan plan) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(plan.inputTopic(), Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()).withName("input"))
            .mapValues(plan::transform, Named.as("project-and-filter"))
            .filter((key, value) -> value != null, Named.as("drop-filtered-and-tombstones"))
            .to(plan.outputTopic(), Produced.with(Serdes.ByteArray(), Serdes.ByteArray()).withName("output"));
        return builder.build();
    }

    public static Properties config(String bootstrap, String applicationId, String stateDir) {
        if (!applicationId.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,199}")) {
            throw new IllegalArgumentException("Invalid application.id");
        }
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        // Kafka Streams fixes allow.auto.create.topics=false; don't supply that controlled config.
        props.put("processing.exception.handler.global.enabled", true);
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            "org.apache.kafka.streams.errors.LogAndFailExceptionHandler");
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
            "org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler");
        return props;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: java -jar proof.jar PLAN_JSON BOOTSTRAP APPLICATION_ID STATE_DIR");
            System.exit(2);
        }
        // Plan validation and topology construction precede creation of any Kafka client.
        Plan plan = new Plan(Files.readAllBytes(Path.of(args[0])));
        Topology topology = topology(plan);
        Properties props = config(args[1], args[2], args[3]);
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean(false);
        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            streams.setUncaughtExceptionHandler(error -> {
                failed.set(true);
                System.err.println("PROOF_PROCESSING_FAILED");
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            streams.setStateListener((next, previous) -> {
                if (next == KafkaStreams.State.RUNNING) System.out.println("PROOF_RUNNING");
                if (next == KafkaStreams.State.ERROR) failed.set(true);
                if (next == KafkaStreams.State.ERROR || next == KafkaStreams.State.NOT_RUNNING) done.countDown();
            });
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                boolean closed = streams.close(CloseOptions.timeout(Duration.ofSeconds(15))
                    .withGroupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP));
                System.out.println(closed ? "PROOF_CLOSED" : "PROOF_CLOSE_TIMEOUT");
                if (!closed) failed.set(true);
                done.countDown();
            }, "proof-shutdown"));
            streams.start();
            done.await();
        }
        if (failed.get()) System.exit(3);
    }
}
