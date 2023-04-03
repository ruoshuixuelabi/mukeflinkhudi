package com.pk.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class PKAvgValueStateFunction extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Double>> {
    /**
     * Tuple2<Long, Long>
     * <p>
     * 第一个元素 存放 个数
     * <p>
     * 第二个元素 存放 和
     */
    private transient ValueState<Tuple2<Long, Long>> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        valueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Tuple2<Long, Long>>("average",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        })
                )
        );
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Double>> out) throws Exception {
        Tuple2<Long, Long> state = valueState.value();
        Tuple2<Long, Long> current = null;
        if (null != state) {
            current = state;
        }
        else {
            current = Tuple2.of(0L, 0L);
        }
        Tuple2<Long, Long> newState = Tuple2.of(current.f0 + 1, current.f1 + value.f1);
        valueState.update(newState);
        if (newState.f0 >= 3) {
            out.collect(Tuple2.of(value.f0, newState.f1.doubleValue() / newState.f0));
            // 清理state
            valueState.clear();
        }
    }
}