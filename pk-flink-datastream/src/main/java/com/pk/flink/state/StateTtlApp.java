package com.pk.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 关于Flink状态的有效期设置
 */
public class StateTtlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.socketTextStream("localhost", 9527)
                .map(String::toLowerCase)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] splits = value.split(",");
                        for (String word : splits) {
                            out.collect(Tuple2.of(word.trim(), 1L));
                        }
                    }
                }).keyBy(x -> x.f0)
                // keyby  过期是作用到key上    某些key时间到了过期，某些key时间没到，就不过期
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                    private transient ValueState<Long> counter = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
                                .updateTtlOnCreateAndWrite()
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                                .build();
                        ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<Long>("state", Long.class);
                        stateDescriptor.enableTimeToLive(ttlConfig);
                        counter = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                        Long history = counter.value(); // 获取状态里面的值
                        long currentValue = value.f1;
                        if (null == history) {
                            history = 0L;
                        }
                        long total = history + currentValue;
                        counter.update(total); // 更新状态的值
                        return Tuple2.of(value.f0, total);
                    }
                }).print();
        env.execute("StateTtlApp");
    }
}