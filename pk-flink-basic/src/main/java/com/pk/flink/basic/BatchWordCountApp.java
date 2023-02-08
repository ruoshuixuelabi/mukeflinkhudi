package com.pk.flink.basic;

import com.pk.flink.basic.function.PKFlatMapFunction;
import com.pk.flink.basic.function.PKMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用Flink进行批处理，并统计wc
 */
public class BatchWordCountApp {
    public static void main(String[] args) throws Exception {
        // step0: Spark中有上下文，Flink中也有上下文，MR中也有
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // step1: 读取文件内容  ==> 一行一行的字符串而已
        DataSource<String> source = env.readTextFile("data/wc.data");
        // step2: 每一行的内容按照指定的分隔符进行拆分  1:N 的关系
        source.flatMap(new PKFlatMapFunction())
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .groupBy(0)  // step4: 按照单词进行分组  groupBy是离线的api
                .sum(1)  // ==> 求词频 sum
                .print();
    }
}