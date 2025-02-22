FROM openjdk:24-slim-bookworm
WORKDIR /app
COPY target/insert-podcast-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"] 