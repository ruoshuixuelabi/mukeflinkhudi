package com.pk.flink.scenario02;

import com.alibaba.fastjson.JSON;
import com.pk.flink.scenario01.Access;
import com.pk.flink.scenario01.PKRedisSink;
import com.pk.flink.utils.DateUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 对接Kafka数据然后使用Flink进行实时统计分析
 */
public class FlinkKafkaTopAnalysisApp {

    public static void main(String[] args) throws Exception {

        // localhost:8081
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        String brokers = "hadoop000:9093,hadoop000:9094,hadoop000:9095";
        String topic = "test11";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId("pk-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "PK-KAFKA-SOURCE");

        SingleOutputStreamOperator<Tuple3<String, String, Long>> resultStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                return JSON.parseObject(value, Access.class);
            }
        }).map(new MapFunction<Access, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(Access x) throws Exception {
                return Tuple3.of(x.getName(), DateUtils.ts2Date(x.getTs(), "yyyyMMdd"), 1L);
            }
        }).keyBy(new KeySelector<Tuple3<String, String, Long>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Long> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2)
                .map(new MapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(Tuple3<String, String, Long> value) throws Exception {
                        return Tuple3.of("pk-access-"+value.f1, value.f0, value.f2);
                    }
                });

        resultStream.addSink(new PKRedisSink());

        env.execute("FlinkKafkaTopAnalysisApp");
    }
}
