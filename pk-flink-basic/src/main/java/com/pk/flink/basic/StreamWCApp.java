package com.pk.flink.basic;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWCApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("data/wc.data");
        source.flatMap((String value, Collector<Tuple2<String,Integer>> out) -> {
            String[] splits = value.split(",");
            for(String split : splits) {
                out.collect(Tuple2.of(split.trim(), 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0) // 这种写法一定要掌握
                .sum(1).print();

        env.execute("作业名字");
    }
}