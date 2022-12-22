package com.pk.flink.wm;

import com.pk.flink.bean.Access;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WMApp01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 事件时间,domain,traffic
        DataStreamSource<String> source = env.socketTextStream("hadoop000", 9527);
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.split(",")[0]));
        source.assignTimestampsAndWatermarks(watermarkStrategy)
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        String[] splits = value.split(",");
                        Access access = new Access();
                        access.setTime(Long.parseLong(splits[0].trim()));
                        access.setDomain(splits[1].trim());
                        access.setTraffic(Double.parseDouble(splits[2].trim()));
                        return access;
                    }
                }).setParallelism(2)
                .process(new ProcessFunction<Access, Access>() {
                    @Override
                    public void processElement(Access value, Context ctx, Collector<Access> out) throws Exception {
                        long watermark = ctx.timerService().currentWatermark();
                        System.out.println("该数据是:" + value + " , WM是:" + watermark);
                        out.collect(value);
                    }
                }).setParallelism(1).print();
        env.execute("WMApp01");
    }
}