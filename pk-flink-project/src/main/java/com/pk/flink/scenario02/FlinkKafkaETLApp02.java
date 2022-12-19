package com.pk.flink.scenario02;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
