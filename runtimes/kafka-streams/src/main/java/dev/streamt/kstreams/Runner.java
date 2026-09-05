package dev.streamt.kstreams;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

/** One foreground process and one fixed topology. The caller owns deployment and source offsets. */
public final class Runner {
    public static final String VERSION = "0.1.1";
    private static final int MAX_PLAN_BYTES = 1_048_576;
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(15);

    record Arguments(Path plan, Path clientProperties, String applicationId, Path stateDirectory,
                     IdentityGate.Expected expected, boolean validateOnly) { }

    interface IdentityVerifier {
        IdentityGate.Expected verify(Properties properties, Plan plan, IdentityGate.Expected expected);
    }

    static Arguments arguments(String[] args) {
        Map<String, String> values = new HashMap<>();
        boolean validate = false;
        for (int index = 0; index < args.length; index++) {
            String key = args[index];
            if (key.equals("--validate-only")) {
                if (validate) throw new IllegalArgumentException("duplicate_argument");
                validate = true;
            } else {
                if (!Set.of("--plan", "--client-properties", "--application-id", "--state-dir",
                        "--expected-cluster-id", "--expected-input-topic-id", "--expected-output-topic-id").contains(key)
                        || index + 1 == args.length
                        || values.putIfAbsent(key, args[++index]) != null) {
                    throw new IllegalArgumentException("invalid_argument");
                }
            }
        }
        if (values.size() != 7 || !values.get("--application-id").matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,199}")) {
            throw new IllegalArgumentException("required_arguments_missing_or_invalid");
        }
        Path state = Path.of(values.get("--state-dir"));
        if (!state.isAbsolute()) throw new IllegalArgumentException("state_directory_must_be_absolute");
        return new Arguments(Path.of(values.get("--plan")), Path.of(values.get("--client-properties")),
            values.get("--application-id"), state,
            new IdentityGate.Expected(values.get("--expected-cluster-id"), values.get("--expected-input-topic-id"),
                values.get("--expected-output-topic-id")), validate);
    }

    public static Topology topology(Plan plan) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(plan.inputTopic(), Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()).withName("input"))
            .mapValues(plan::transform, Named.as("project-and-filter"))
            .filter((key, value) -> value != null, Named.as("drop-filtered-and-tombstones"))
            .to(plan.outputTopic(), Produced.with(Serdes.ByteArray(), Serdes.ByteArray()).withName("output"));
        return builder.build();
    }

    static String failureReason(Throwable error) {
        Throwable current = error;
        for (int depth = 0; current != null && depth < 16; depth++, current = current.getCause()) {
            if (current instanceof NoOffsetForPartitionException || current instanceof OffsetOutOfRangeException) {
                return "missing_or_invalid_offsets";
            }
        }
        return "processing_failed";
    }

    public static int execute(String[] args) {
        return execute(args, IdentityGate::verify);
    }

    static int execute(String[] args, IdentityVerifier verifier) {
        if (args.length == 1 && args[0].equals("--version")) {
            System.out.println("{\"runner_version\":\"" + VERSION + "\",\"plan_version\":1,\"kafka_version\":\"4.3.1\"}");
            return 0;
        }
        String stage = "invalid_arguments";
        try {
            Arguments arguments = arguments(args);
            stage = "plan_invalid";
            byte[] planBytes;
            try (var input = Files.newInputStream(arguments.plan())) { planBytes = input.readNBytes(MAX_PLAN_BYTES + 1); }
            if (planBytes.length > MAX_PLAN_BYTES) throw new IllegalArgumentException("plan_too_large");
            Plan plan = new Plan(planBytes);
            String hash = "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(planBytes));
            Topology topology = topology(plan);
            stage = "client_properties_invalid";
            Properties connection = ClientProperties.read(arguments.clientProperties());
            ClientProperties.validateLocalTls(connection);
            Properties configuration = ClientProperties.streams(connection, arguments.applicationId(), arguments.stateDirectory());
            if (arguments.validateOnly()) {
                System.out.println("{\"state\":\"validated\",\"runner_version\":\"" + VERSION + "\",\"plan_version\":1,\"plan_sha256\":\"" + hash + "\"}");
                return 0;
            }
            stage = "identity_verification_failed";
            IdentityGate.Expected verified = verifier.verify(connection, plan, arguments.expected());
            stage = "local_state_unavailable";
            StatusFile status = new StatusFile(arguments.stateDirectory(), arguments.applicationId(), hash, verified);
            status.transition("starting", null);
            return run(topology, configuration, status);
        } catch (Exception failure) {
            // No exception message, cause, stack trace, argument, plan or properties are printed.
            System.err.println("{\"state\":\"failed\",\"reason\":\"" + stage + "\"}");
            return 2;
        }
    }

    private static int run(Topology topology, Properties configuration, StatusFile status) {
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> failure = new AtomicReference<>();
        AtomicBoolean closing = new AtomicBoolean(false);
        KafkaStreams streams;
        try { streams = new KafkaStreams(topology, configuration); }
        catch (RuntimeException invalid) {
            publish(status, "failed", "startup_failed", failure);
            return 3;
        }
        Runnable close = () -> {
            if (!closing.compareAndSet(false, true)) return;
            publish(status, "closing", null, failure);
            boolean closed = streams.close(CloseOptions.timeout(CLOSE_TIMEOUT)
                .withGroupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP));
            if (!closed) failure.compareAndSet(null, "shutdown_timeout");
            publish(status, failure.get() == null ? "closed" : "failed", failure.get(), failure);
            done.countDown();
        };
        Thread shutdown = new Thread(close, "streamt-runner-shutdown");
        try {
            streams.setUncaughtExceptionHandler(error -> {
                failure.compareAndSet(null, failureReason(error));
                publish(status, "failed", failure.get(), failure);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            streams.setStateListener((next, previous) -> {
                if (next == KafkaStreams.State.RUNNING && !closing.get()) {
                    publish(status, "running", null, failure);
                    ready.countDown();
                }
                if (next == KafkaStreams.State.ERROR) {
                    failure.compareAndSet(null, "runtime_failed");
                    publish(status, "failed", failure.get(), failure);
                }
                if (next == KafkaStreams.State.ERROR || next == KafkaStreams.State.NOT_RUNNING) {
                    ready.countDown();
                    done.countDown();
                }
            });
            Runtime.getRuntime().addShutdownHook(shutdown);
            streams.start();
            if (!ready.await(30, TimeUnit.SECONDS)) {
                failure.compareAndSet(null, "startup_timeout");
                publish(status, "failed", failure.get(), failure);
            } else if (failure.get() == null) done.await();
        } catch (Exception invalid) {
            if (invalid instanceof InterruptedException) Thread.currentThread().interrupt();
            failure.compareAndSet(null, "runtime_failed");
            publish(status, "failed", failure.get(), failure);
        } finally {
            close.run();
            try { Runtime.getRuntime().removeShutdownHook(shutdown); }
            catch (IllegalStateException shuttingDown) { /* SIGTERM shutdown is already in progress. */ }
        }
        return failure.get() == null ? 0 : 3;
    }

    private static void publish(StatusFile status, String state, String reason, AtomicReference<String> failure) {
        try { status.transition(state, reason); }
        catch (Exception unavailable) {
            failure.compareAndSet(null, "status_write_failed");
            System.err.println("{\"state\":\"failed\",\"reason\":\"status_write_failed\"}");
        }
    }

    public static void main(String[] args) { System.exit(execute(args)); }
}
