package com.pk.flink.scenario03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * 功能需求：使用Flink对接Kafka的数据源，完成精准一次语义
 * <p>
 * Flink + Kafka(offset)
 * 对于Flink来说，offset在State管理中完成
 * <p>
 * <p>
 * 问题：
 * 1) 每次开发一个Flink+Kafka应用程序，你都得这么写代码， 费劲不？
 * 2) IDEA扫描发现，你的工程中是有很多的重复的代码的
 * ==>
 * 在工作中有一个"潜"规则: 当一段代码在多个代码中有重复， 就得想着我是不是该把代码重构下
 */
public class EOSApp01 {
    public static void main(String[] args) throws Exception {
        DataStream<String> stream = FlinkUtils.createStream(args, SimpleStringSchema.class);
        // TODO... 完成业务逻辑开发
        stream.print();
        FlinkUtils.environment.execute("EOSApp01");
    }

    public static void test01() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1 硬编码   ==> 通过配置文件
        String brokers = "hadoop000:9093,hadoop000:9094,hadoop000:9095";
        String topic = "pkflink";
        String group = "pkgroup";
        // 2 对接Kafka数据源 代码重复问题
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(group)
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> stream = environment.fromSource(source, WatermarkStrategy.noWatermarks(), "pk-kafka-source");
        // TODO... 完成业务逻辑开发
        stream.print();
        environment.execute("EOSApp01");
    }
}