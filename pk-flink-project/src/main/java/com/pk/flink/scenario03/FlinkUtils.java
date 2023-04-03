package com.pk.flink.scenario03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class FlinkUtils {
    public static StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T> createStream(String[] args, Class<? extends DeserializationSchema<T>> deserializationSchema) throws Exception {
        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);
        String group = tool.get("group", "pkgroup");
        String brokers = tool.getRequired("brokers");
        String topics = tool.getRequired("topics");
        environment.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        environment.getCheckpointConfig().setCheckpointStorage("file:///Users/rocky/source/flinkworkspace/pk-flink/chk");
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5));
        environment.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2 对接Kafka数据源 代码重复问题
        KafkaSource<T> source = KafkaSource.<T>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(group)
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(deserializationSchema.newInstance())
                .build();
        return environment.fromSource(source, WatermarkStrategy.noWatermarks(), "pk-kafka-source");
    }
}