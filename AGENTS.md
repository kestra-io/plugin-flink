# Kestra Flink Plugin

## What

- Provides plugin components under `io.kestra.plugin.flink`.
- Includes classes such as `MonitorJob`, `CancelJob`, `TriggerSavepoint`, `Submit`.

## Why

- This plugin integrates Kestra with Apache Flink.
- It provides tasks that submit, monitor, cancel, and trigger savepoints for Apache Flink jobs over the REST API.

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

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
