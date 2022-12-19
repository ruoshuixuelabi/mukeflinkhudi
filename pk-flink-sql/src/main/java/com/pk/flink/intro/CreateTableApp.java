package com.pk.flink.intro;

import com.alibaba.fastjson.JSON;
import com.pk.flink.bean.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * 创建Table对象
 */
public class CreateTableApp {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从已经存在的表来获取Table对象
//        Table table1 = tableEnv.from("");

        // 从Connector获取Table对象
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("gender", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .build();


//         {"id":1, "name":"PK", "gender":"M", "age":10}
//        TableDescriptor tableDescriptor = TableDescriptor.forConnector("kafka")
//                .schema(schema)
//                .option("topic", "test05")
//                .option("properties.bootstrap.servers", "hadoop000:9093,hadoop000:9094,hadoop000:9095")
//                .option("properties.group.id", "g12")
//                .option("scan.startup.mode", "latest-offset")
//                .format("json")
//                .build();
//
//
//        tableEnv.createTable("table_a", tableDescriptor);
//        tableEnv.executeSql("desc table_a").print();
//        tableEnv.executeSql("select gender, max(age) max_age from table_a group by gender").print();



//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers("hadoop000:9093,hadoop000:9094,hadoop000:9095")
//                .setTopics("test05")
//                .setGroupId("g12")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//        // kafkaStream  String  f0      我们需要的是id name
//
//        DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "pk-kafka-source");
//
//        Table table1 = tableEnv.fromDataStream(kafkaStream.map(x -> JSON.parseObject(x, Person.class)),
//                Schema.newBuilder()
//                .column("id", DataTypes.INT())
//                .column("name", DataTypes.STRING())
//                .build());
//
//        table1.printSchema();
//        table1.execute().print();


        tableEnv.executeSql("create table ....");
    }
}
