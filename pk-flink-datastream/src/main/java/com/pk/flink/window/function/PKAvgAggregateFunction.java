package com.pk.flink.window.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class PKAvgAggregateFunction implements AggregateFunction<Tuple2<String,Long>, Tuple2<Long,Long>, Double> {


    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return Tuple2.of(0L, 0L);
    }

    /**
     * 把当前进来的元素添加到累加器中，并返回一个全新的累加器
     * @param value  当前进来的元素
     * @param accumulator  累加器
     * @return 新的累加器
     */
    @Override
    public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {

        System.out.println("... add invoked ... " + value.f0 + "===>" + value.f1);

        // (求和， 次数+1)
        return Tuple2.of(accumulator.f0 + value.f1, accumulator.f1 + 1);
    }

    @Override
    public Double getResult(Tuple2<Long, Long> accumulator) {
        return Double.valueOf(accumulator.f0) / accumulator.f1;
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return null;
    }
}
