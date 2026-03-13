# Kestra Flink Plugin

## What

description = 'Apache Flink plugin for Kestra Exposes 5 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Apache Flink, allowing orchestration of Apache Flink-based operations as part of data pipelines and automation workflows.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `flink`

Infrastructure dependencies (Docker Compose services):

- `default`
- `jobmanager`
- `taskmanager`

### Key Plugin Classes

- `io.kestra.plugin.flink.CancelJob`
- `io.kestra.plugin.flink.MonitorJob`
- `io.kestra.plugin.flink.Submit`
- `io.kestra.plugin.flink.SubmitSql`
- `io.kestra.plugin.flink.TriggerSavepoint`

### Project Structure

```
plugin-flink/
├── src/main/java/io/kestra/plugin/flink/
├── src/test/java/io/kestra/plugin/flink/
├── build.gradle
└── README.md
```

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
