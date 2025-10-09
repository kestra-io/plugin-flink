<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png"  alt="Kestra workflow orchestrator" />
  </a>
</p>

<h1 align="center" style="border-bottom: none">
    Apache Flink Plugin for Kestra
</h1>

<div align="center">
 <a href="https://github.com/kestra-io/kestra/releases"><img src="https://img.shields.io/github/tag-pre/kestra-io/kestra.svg?color=blueviolet" alt="Last Version" /></a>
  <a href="https://github.com/kestra-io/kestra/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/kestra-io/kestra?color=blueviolet" alt="License" /></a>
  <a href="https://github.com/kestra-io/kestra/stargazers"><img src="https://img.shields.io/github/stars/kestra-io/kestra?color=blueviolet&logo=github" alt="Github star" /></a> <br>
<a href="https://kestra.io"><img src="https://img.shields.io/badge/Website-kestra.io-192A4E?color=blueviolet" alt="Kestra infinitely scalable orchestration and scheduling platform"></a>
<a href="https://kestra.io/slack"><img src="https://img.shields.io/badge/Slack-Join%20Community-blueviolet?logo=slack" alt="Slack"></a>
</div>

<br />

<p align="center">
  <a href="https://twitter.com/kestra_io" style="margin: 0 10px;">
        <img src="https://kestra.io/twitter.svg" alt="twitter" width="35" height="25" /></a>
  <a href="https://www.linkedin.com/company/kestra/" style="margin: 0 10px;">
        <img src="https://kestra.io/linkedin.svg" alt="linkedin" width="35" height="25" /></a>
  <a href="https://www.youtube.com/@kestra-io" style="margin: 0 10px;">
        <img src="https://kestra.io/youtube.svg" alt="youtube" width="35" height="25" /></a>
</p>

<br />

This plugin provides tasks for orchestrating Apache Flink jobs within Kestra workflows. It supports both streaming and batch processing scenarios and integrates with Flink's REST API and SQL Gateway.

## Features

- **Submit Jobs**: Submit JAR-based jobs to Flink clusters
- **SQL Execution**: Execute SQL statements via Flink SQL Gateway
- **Job Monitoring**: Monitor job status and wait for completion
- **Job Cancellation**: Cancel running jobs with optional savepoint creation
- **Savepoint Management**: Trigger savepoints for job state preservation

## Tasks

### Submit
Submits a Flink job using a JAR file to a Flink cluster.

```yaml
- id: submit-flink-job
  type: io.kestra.plugin.flink.Submit
  restUrl: "http://flink-jobmanager:8081"
  jarUri: "s3://flink/jars/my-job.jar"
  entryClass: "com.example.Main"
  args:
    - "--input"
    - "s3://input/data"
  parallelism: 4
```

### SubmitSql
Executes SQL statements via Flink SQL Gateway.

**Note:** For streaming jobs that reach `RUNNING` state, the SQL Gateway session is kept alive to allow the job to continue. Batch jobs that reach `FINISHED` state will have their temporary sessions cleaned up automatically.

```yaml
- id: run-sql
  type: io.kestra.plugin.flink.SubmitSql
  gatewayUrl: "http://flink-sql-gateway:8083"
  statement: |
    INSERT INTO enriched_orders
    SELECT o.order_id, o.customer_id, c.name, o.amount
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
  sessionConfig:
    catalog: "default_catalog"
    database: "default_database"
    configuration:
      execution.runtime-mode: "streaming"
```

### MonitorJob
Monitors a Flink job until it reaches a terminal state.

```yaml
- id: monitor-job
  type: io.kestra.plugin.flink.MonitorJob
  restUrl: "http://flink-jobmanager:8081"
  jobId: "{{ outputs.submit-flink-job.jobId }}"
  waitTimeout: "PT30M"
```

### Cancel
Cancels a running Flink job with optional savepoint creation.

```yaml
- id: cancel-job
  type: io.kestra.plugin.flink.Cancel
  restUrl: "http://flink-jobmanager:8081"
  jobId: "{{ inputs.jobId }}"
  withSavepoint: true
  savepointDir: "s3://flink/savepoints/canceled"
```

### TriggerSavepoint
Triggers a savepoint for a running job without canceling it.

```yaml
- id: create-savepoint
  type: io.kestra.plugin.flink.TriggerSavepoint
  restUrl: "http://flink-jobmanager:8081"
  jobId: "{{ inputs.jobId }}"
  targetDirectory: "s3://flink/savepoints/backup"
```

## Requirements

- Flink cluster with REST API enabled (default port 8081)
- For SQL tasks: Flink SQL Gateway (default port 8083)
- Network connectivity from Kestra to Flink cluster
- Appropriate permissions for savepoint directories (if using external storage)


## Documentation
* Full documentation can be found under: [kestra.io/docs](https://kestra.io/docs)
* Documentation for developing a plugin is included in the [Plugin Developer Guide](https://kestra.io/docs/plugin-developer-guide/)


## License
Apache 2.0 © [Kestra Technologies](https://kestra.io)


## Stay up to date

We release new versions every month. Give the [main repository](https://github.com/kestra-io/kestra) a star to stay up to date with the latest releases and get notified about future updates.

![Star the repo](https://kestra.io/star.gif)
