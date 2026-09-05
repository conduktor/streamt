FROM maven:3.9.11-eclipse-temurin-21@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237 AS build
WORKDIR /build
COPY pom.xml ./
COPY src ./src
RUN mvn -B -q package

FROM eclipse-temurin:21-jre@sha256:7a65df4b22d2de92d4e04056e884f3b9122d70b21e2847fd66084278bd0ce037
LABEL io.streamt.runner.version="0.1.1" io.streamt.plan.version="1"
RUN mkdir -p /opt/streamt/runner /run/streamt /var/lib/streamt/state \
    && chown -R 10001:10001 /opt/streamt /run/streamt /var/lib/streamt
COPY --from=build --chown=10001:10001 /build/target/streamt-kafka-streams-runner-0.1.1.jar /opt/streamt/runner/runner.jar
COPY --from=build --chown=10001:10001 /build/target/lib /opt/streamt/runner/lib
COPY LICENSE /opt/streamt/runner/LICENSE
USER 10001:10001
WORKDIR /opt/streamt/runner
STOPSIGNAL SIGTERM
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/opt/streamt/runner/runner.jar"]
