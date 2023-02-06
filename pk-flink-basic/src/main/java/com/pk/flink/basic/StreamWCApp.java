package com.pk.flink.basic;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink以流处理的方式实现功能开发
 */
public class StreamWCApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //下面方法已经废弃
//        DataStreamSource<String> source = env.readTextFile("data/wc.data");
        //目前新版本推荐使用这种方法
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("data/wc.data"))
                .build();
        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "MySourceName");
        stream.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        out.collect(Tuple2.of(split.trim(), 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0) // 这种写法一定要掌握
                .sum(1).print();
        //对于流处理需要执行才能打印
        env.execute("作业名字");
    }
}