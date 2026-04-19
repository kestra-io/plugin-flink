# Kestra Flink Plugin

## What

- Provides plugin components under `io.kestra.plugin.flink`.
- Includes classes such as `MonitorJob`, `CancelJob`, `TriggerSavepoint`, `Submit`.

## Why

- What user problem does this solve? Teams need to submit, monitor, cancel, and trigger savepoints for Apache Flink jobs over the REST API from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Apache Flink steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Apache Flink.

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
