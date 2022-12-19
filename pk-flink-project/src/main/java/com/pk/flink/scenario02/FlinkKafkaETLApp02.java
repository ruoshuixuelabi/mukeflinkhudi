package com.pk.flink.scenario02;

import com.alibaba.fastjson.JSON;
import com.pk.flink.scenario01.Access;
import com.pk.flink.utils.MySQLUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 使用Flink异步IO的操作
 */
public class FlinkKafkaETLApp02 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 1  2  3  4  5
        DataStreamSource<String> lines = env.socketTextStream("hadoop000", 9527);

        int capacity = 20;

        SingleOutputStreamOperator<Tuple2<String, String>> result = AsyncDataStream.orderedWait(lines,
                new MySQLAsyncFunction(capacity),
                3000,
                TimeUnit.MILLISECONDS,
                capacity
        );

        result.print();

        env.execute("FlinkKafkaETLApp");
    }
}
