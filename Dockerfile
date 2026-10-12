FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre
# An unprivileged user that owns nothing but the data directory.
RUN groupadd --system redis \
    && useradd --system --gid redis --home-dir /data --shell /usr/sbin/nologin redis \
    && mkdir /data && chown redis:redis /data
COPY --from=build /src/target/redis-java.jar /app/redis-java.jar
COPY --chmod=755 docker/healthcheck.sh /app/healthcheck.sh
USER redis
WORKDIR /data
VOLUME /data
EXPOSE 6379
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s CMD ["/app/healthcheck.sh"]
# The dataset lives on the heap, so let it have most of the container's memory.
# Running out of heap mid-command could leave the keyspace half-updated; exiting
# and restarting from the last snapshot is the safer failure (Redis aborts too).
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", \
            "-jar", "/app/redis-java.jar", "--dir", "/data"]
