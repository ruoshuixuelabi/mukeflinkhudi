package com.pk.flink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 需要使用Flink内置的ValueState来进行状态的管理
 * <p>
 * 当收到的相同的key的元素个数=3， 求value的平均值
 * <p>
 * <p>
 * if(xx.size == 3) {
 * 平均值 = 和 / 元素个数
 * }
 * <p>
 * <p>
 * 关于ValueState的数据操作API：
 * value
 * update
 * clear
 */
public class ValueStateApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<String, Long>> list = new ArrayList<>();
        list.add(Tuple2.of("PK", 1L));
        list.add(Tuple2.of("PK", 7L));
        list.add(Tuple2.of("ruoze", 4L));
        list.add(Tuple2.of("PK", 7L));
        list.add(Tuple2.of("ruoze", 2L));
        list.add(Tuple2.of("ruoze", 4L));
        /*
         * PK : 5.0
         */
        env.fromCollection(list)
                .keyBy(x -> x.f0)
                .flatMap(new PKAvgValueStateFunction())
                .print();
        env.execute("ValueStateApp");
    }
}