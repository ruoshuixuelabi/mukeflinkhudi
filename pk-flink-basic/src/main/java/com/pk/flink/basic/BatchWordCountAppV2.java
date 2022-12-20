package com.pk.flink.basic;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCountAppV2 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = env.readTextFile("data/wc.data");
        /*
         * lambda语法： (参数1，参数2，参数3...) -> {函数体}
         */
//        source.map(String::toUpperCase).print();
        // 使用了Java泛型，由于泛型擦除的原因，需要显示的声明类型信息
        source.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        out.collect(Tuple2.of(split.trim(), 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0).sum(1).print();
    }
}