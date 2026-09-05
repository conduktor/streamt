package dev.streamt.proof;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class LifecycleTest {
    @ParameterizedTest @ValueSource(strings = {"-version", "--not-a-valid-java-option"})
    void alreadyExitedChildCannotSatisfyCleanUpdate(String argument, @TempDir Path directory) throws Exception {
        Path log = directory.resolve("child.log");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), argument)
            .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        assertTrue(child.waitFor(10, TimeUnit.SECONDS));
        // Even a stale success marker must not turn an already-exited child into a clean transition.
        Files.writeString(log, "PROOF_CLOSED\n");
        assertThrows(AssertionError.class, () -> RealKafkaTest.requireCleanStop(child, log));
    }
}
