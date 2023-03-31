package com.pk.flink.wm;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 数据延迟/乱序 三种
 * <p>
 * 小：容忍度
 * <p>
 * 中：allowedLateness
 * <p>
 * 大：sideOutputLateData
 * <p>
 * 一起使用
 */
public class WMWindowApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("late-data") {};
        // 事件时间,domain,traffic  开窗口  groupby  求窗口内每个domain出现的次数
        DataStreamSource<String> source = env.socketTextStream("hadoop000", 9527);
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.split(",")[0].trim()));
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = source.assignTimestampsAndWatermarks(watermarkStrategy)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] splits = value.split(",");
                        return Tuple2.of(splits[1].trim(), Integer.parseInt(splits[2].trim()));
                    }
                }).keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(outputTag)
                .sum(1);
        //获取
        DataStream<Tuple2<String, Integer>> sideOutput = result.getSideOutput(outputTag);
        //延迟的数据会输出到测流
        sideOutput.print("-----side output------");
        result.print();
        /*
         * 滑动窗口大小是6秒，每隔2秒滑动一次
         *
         * [0,2)
         * [0,4)
         * [0,6)
         */
        /*
         * [window_start, window_end)
         * [0000,5000)
         * Watermark >= window_end 就会触发前面的执行
         * 4999 >= 4999
         * [5000,10000)
         * 11999 >= 9999
         */
        env.execute("WMApp01");
    }
}