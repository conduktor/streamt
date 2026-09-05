"""Build and run the proof on an owned disposable broker, then remove only that broker."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

from compile_plan import compile_plan

ROOT = Path(__file__).resolve().parent
IMAGE = "apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
VERSION = "4.3.1"


def run(
    args: list[str],
    *,
    capture_output: bool = False,
    timeout: int = 300,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - locally constructed argv; no shell
        args,
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=capture_output,
        timeout=timeout,
        env=env,
    )


def cleanup_owned_broker(name: str, token: str) -> list[str]:
    owner = run(
        ["docker", "inspect", name, "--format", '{{ index .Config.Labels "streamt.proof.owner" }}'],
        capture_output=True,
        timeout=10,
    ).stdout.strip()
    if name != f"streamt-kstreams-proof-{token}" or owner != token:
        raise RuntimeError(f"Refusing cleanup: ownership mismatch for {name}")
    mounts = json.loads(
        run(
            ["docker", "inspect", name, "--format", "{{json .Mounts}}"],
            capture_output=True,
            timeout=10,
        ).stdout
    )
    volumes = [mount["Name"] for mount in mounts if mount["Type"] == "volume"]
    # -v removes this container's anonymous volumes; no explicit volumes are mounted by the fixture.
    run(["docker", "rm", "-f", "-v", name], capture_output=True, timeout=30)
    remaining = set(
        run(
            ["docker", "volume", "ls", "--format", "{{.Name}}"], capture_output=True, timeout=10
        ).stdout.splitlines()
    )
    if remaining.intersection(volumes):
        raise RuntimeError("Owned broker removed but one of its recorded volumes remains")
    return volumes


def main() -> None:
    # Compile before Docker or Kafka. No runtime can silently reinterpret unsupported SQL.
    token = uuid.uuid4().hex[:12]
    name = f"streamt-kstreams-proof-{token}"
    source, output = f"proof-{token}.input", f"proof-{token}.output"
    schema = json.loads((ROOT / "examples/schema.json").read_text())
    plans = {
        "plan.json": compile_plan(
            (ROOT / "examples/orders.sql").read_text(), schema, "orders", source, output
        ),
        "plan-updated.json": compile_plan(
            (ROOT / "examples/orders_updated.sql").read_text(), schema, "orders", source, output
        ),
    }
    run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "--tb=short",
            "-c",
            "/dev/null",
            "-p",
            "no:cacheprovider",
            "test_compile_plan.py",
            "test_run_proof.py",
        ]
    )
    build_env = dict(os.environ)
    build_env.pop("STREAMT_PROOF_BOOTSTRAP", None)
    run(["mvn", "-q", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "package"], env=build_env)
    evidence = ROOT / "target/real-proof" / token
    evidence.mkdir(parents=True, exist_ok=True)
    for filename, plan in plans.items():
        (evidence / filename).write_text(json.dumps(plan, indent=2) + "\n")
    # Reserve an available loopback port. Docker rejects a collision if another process wins it.
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    bootstrap = f"127.0.0.1:{port}"
    sys.stdout.write(f"Owned broker: {name}; bootstrap: {bootstrap}; image: {IMAGE}\n")
    sys.stdout.flush()
    broker_env = {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "EXTERNAL://:9092,INTERNAL://:19092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"EXTERNAL://{bootstrap},INTERNAL://localhost:19092",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
        "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m",
    }
    args = [
        "docker",
        "create",
        "--name",
        name,
        "--label",
        f"streamt.proof.owner={token}",
        "-p",
        f"127.0.0.1:{port}:9092",
    ]
    for key, value in broker_env.items():
        args.extend(["-e", f"{key}={value}"])
    created = False
    try:
        run([*args, IMAGE], capture_output=True, timeout=120)
        created = True
        run(["docker", "start", name], capture_output=True, timeout=30)
        actual = run(
            ["docker", "exec", name, "/opt/kafka/bin/kafka-topics.sh", "--version"],
            capture_output=True,
            timeout=20,
        ).stdout.strip()
        if actual.split()[0] != VERSION:
            raise RuntimeError(f"Broker version mismatch before topic creation: {actual}")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            logs = run(["docker", "logs", name], capture_output=True, timeout=10)
            if "Kafka Server started" in logs.stdout + logs.stderr:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("Broker readiness timed out")
        metadata = {
            "container": name,
            "bootstrap": bootstrap,
            "image": IMAGE,
            "broker_version": actual,
        }
        (evidence / "broker.json").write_text(json.dumps(metadata, indent=2) + "\n")
        test_env = dict(
            os.environ,
            STREAMT_PROOF_BOOTSTRAP=bootstrap,
            STREAMT_PROOF_APPLICATION_ID=name,
            STREAMT_PROOF_EVIDENCE=str(evidence),
        )
        run(
            [
                "mvn",
                "-q",
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
                "-Dtest=RealKafkaTest",
                "test",
            ],
            env=test_env,
            timeout=210,
        )
        sys.stdout.write((evidence / "result.json").read_text() + "\n")
    finally:
        if created:
            try:
                logs = run(["docker", "logs", name], capture_output=True, timeout=15)
                (evidence / "broker.log").write_text(logs.stdout + logs.stderr)
            finally:
                # A log collection failure must not prevent owner-checked cleanup.
                volumes = cleanup_owned_broker(name, token)
                (evidence / "cleanup.json").write_text(
                    json.dumps(
                        {
                            "container": name,
                            "recorded_volume_names": volumes,
                            "container_removed": True,
                            "recorded_volumes_absent": True,
                        },
                        indent=2,
                    )
                    + "\n"
                )
                sys.stdout.write(
                    f"Removed owned broker {name} and verified its {len(volumes)} volumes absent; logs retained in {evidence}\n"
                )
                sys.stdout.flush()


if __name__ == "__main__":
    main()
