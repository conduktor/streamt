package dev.streamt.kstreams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Set;

/** Only fixed event fields; records, connection values and exception messages never enter status. */
public final class StatusFile {
    private final Path directory;
    private final String applicationId;
    private final String planHash;
    private final IdentityGate.Expected verified;
    private boolean failed;

    public StatusFile(Path directory, String applicationId, String planHash, IdentityGate.Expected verified) throws IOException {
        this.directory = directory;
        this.applicationId = applicationId;
        this.planHash = planHash;
        this.verified = java.util.Objects.requireNonNull(verified);
        Files.createDirectories(directory);
    }

    public synchronized void transition(String state, String reason) throws IOException {
        if (!Set.of("starting", "running", "closing", "closed", "failed").contains(state)) {
            throw new IllegalArgumentException("invalid_status_state");
        }
        // Failure evidence survives shutdown callbacks and remains visible to the orchestrator.
        if (failed && !state.equals("failed")) return;
        failed = state.equals("failed");
        var body = Plan.JSON.createObjectNode()
            .put("runner_version", Runner.VERSION).put("plan_version", 1)
            .put("application_id", applicationId).put("plan_sha256", planHash)
            .put("cluster_id", verified.clusterId()).put("input_topic_id", verified.inputTopicId())
            .put("output_topic_id", verified.outputTopicId())
            .put("state", state).put("updated_at", Instant.now().toString());
        if (reason == null) body.putNull("reason"); else body.put("reason", reason);
        byte[] bytes = Plan.JSON.writeValueAsBytes(body);
        Path temporary = Files.createTempFile(directory, ".status-", ".json");
        try {
            Files.write(temporary, bytes);
            Files.move(temporary, directory.resolve("status.json"), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
        System.out.println(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
    }
}
