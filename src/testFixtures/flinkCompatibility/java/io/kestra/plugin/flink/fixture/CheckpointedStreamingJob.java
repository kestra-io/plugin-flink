package io.kestra.plugin.flink.fixture;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class CheckpointedStreamingJob {
    private CheckpointedStreamingJob() {
    }

    public static void main(String[] args) throws Exception {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(1_000);
        environment.setParallelism(1);
        environment.fromSequence(1, Long.MAX_VALUE)
            .map(value ->
            {
                Thread.sleep(50);
                return value;
            })
            .print()
            .name("compatibility-fixture-sink");
        environment.execute("flink-client-compatibility-fixture");
    }
}
