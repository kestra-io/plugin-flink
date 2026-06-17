# How to use the Apache Flink plugin

Submit and manage Flink jobs via the JobManager REST API from Kestra flows.

## Authentication

Set `restUrl` to the Flink JobManager REST endpoint (e.g. `http://jobmanager:8081`) on each task, or apply it globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults). The plugin communicates over plain HTTP — no built-in auth fields. If your cluster requires authentication, place a reverse proxy in front of the JobManager.

## Tasks

`Submit` uploads a JAR and submits a job — set `restUrl`, `jarUri`, and `entryClass` (all required). Optionally pass program `args`, set `parallelism`, provide extra Flink configuration via `jobConfig`, or restore from a savepoint with `restoreFromSavepoint`.

`SubmitSql` executes a SQL statement via the Flink SQL Gateway — set `gatewayUrl` and `statement` (both required). Optionally set `sessionName` to reuse a named session and configure catalog/database via `sessionConfig`. Control execution timeout with `statementTimeout` (default 300 seconds).

`CancelJob` stops a running job by `jobId` — set `restUrl` and `jobId` (both required). Set `withSavepoint: true` and `savepointDir` to create a final savepoint before stopping. Set `drainJob: true` to flush buffered data before cancellation.

`TriggerSavepoint` creates a savepoint for a running job — set `restUrl` and `jobId` (both required). Optionally set `targetDirectory` and `cancelJob: true` to stop the job after the savepoint completes.

`MonitorJob` is a polling trigger — set `restUrl` and `jobId` (both required). It fires when the job reaches a terminal state. Control polling with `interval` (default 10 seconds). Set `failOnError: false` to treat a `FAILED` state as non-fatal.
