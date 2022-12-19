package com.pk.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        sessionWindow(env);

        env.execute();
    }

    public static void sessionWindow(StreamExecutionEnvironment env) {
        SingleOutputStreamOperator<Integer> source = env.socketTextStream("localhost", 9527)
                .map(x -> Integer.parseInt(x.trim()));

        source.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(0) // WindowFunction
                .print();

    }

    public static void slidingWindow(StreamExecutionEnvironment env) {
        SingleOutputStreamOperator<Integer> source = env.socketTextStream("localhost", 9527)
                .map(x -> Integer.parseInt(x.trim()));

        /**
         * 窗口大小是10s，滑动大小是5s
         * 0      5     10     15
         * 1：0-5
         * 2：0-10
         * 3：5-15
         *
         */
        source.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(0)
                .print();

    }

    public static void tumblingWindow(StreamExecutionEnvironment env) {

//        SingleOutputStreamOperator<Integer> source = env.socketTextStream("localhost", 9527)
//                .map(x -> Integer.parseInt(x.trim()));
//
//        source.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 5秒一个窗口
//        .sum(0)
//                .print();

        SingleOutputStreamOperator<Tuple2<String, Integer>> source = env.socketTextStream("localhost", 9527)
                .map(x -> {
                    String[] splits = x.split(",");
                    return Tuple2.of(splits[0].trim(), Integer.parseInt(splits[1].trim()));
                }).returns(Types.TUPLE(Types.STRING, Types.INT));

        source.keyBy(x -> x.f0) // 先分组
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 5秒一个窗口
                .sum(1) // WindowFunction
                .print();
    }




    public static void countWindow(StreamExecutionEnvironment env) {
        /**
         * nc端输入数据，每行就一个数字，window为count的，元素个数为5，求和
         */

//        SingleOutputStreamOperator<Integer> source = env.socketTextStream("localhost", 9527)
//                .map(x -> Integer.parseInt(x.trim()));
//
//
//        // countWindowAll的并行度是多少？ 思路：打开UI
//        source.countWindowAll(5)
//                .sum(0) // WindowFunction
//                .print();

        // spark,1   spark,2   spark,3
        SingleOutputStreamOperator<Tuple2<String, Integer>> source = env.socketTextStream("localhost", 9527)
                .map(x -> {
                    String[] splits = x.split(",");
                    return Tuple2.of(splits[0].trim(), Integer.parseInt(splits[1].trim()));
                }).returns(Types.TUPLE(Types.STRING, Types.INT));

        source.keyBy(x -> x.f0) // 先分组
            .countWindow(5) // 5个元素一个窗口
        .sum(1)
                .print();

        /**
         * 注意事项：
         * 对于non-key，只要满足元素个数就会触发作业执行
         * 对于key，每个组达到一定的元素个数才会触发作业执行
         *
         * tumbling count windows
         */
    }
}
