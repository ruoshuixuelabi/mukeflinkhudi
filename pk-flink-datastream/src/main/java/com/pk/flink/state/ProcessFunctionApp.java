package com.pk.flink.state;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * process:
 * 处理单条数据的
 * 针对keyedState的状态编程
 * 整合定时器来进行触发: 同一个key上可以注册多个定时器，触发onTimer的执行
 * <p>
 * 既然我们都可以通过process定义定时器的执行
 * <p>
 * 结合eventime + wm  + 不允许你使用window，而是使用process来完成窗口的统计分析  作为作业来完成
 */
public class ProcessFunctionApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
//                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {
//                    private transient ValueState<Long> valueState = null;
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        ValueStateDescriptor stateDescriptor = new ValueStateDescriptor("state", Long.class);
//                        valueState = getRuntimeContext().getState(stateDescriptor);
//                    }
//                    @Override
//                    public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
//                        Long current = value.f1;
//                        Long history = valueState.value( ) ;if( null == history) {
//                            history = 0L;
//                        }
//                        current +=history;
//                        value.f1=current;
//                        valueState.update(current);
//                        out.collect(value);
//                    }
//                }).print();
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    private transient ValueState<Long> valueState = null;
                    FastDateFormat format = FastDateFormat.getInstance("HH:mm:ss");

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor stateDescriptor = new ValueStateDescriptor("state", Long.class);
                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        long currentTimeMillis = System.currentTimeMillis();
                        long triggerTime = currentTimeMillis + 15000; // 定义了一个定时器，在15秒后触发
                        System.out.println(ctx.getCurrentKey() + "，注册了定时器：" + format.format(triggerTime));
                        ctx.timerService().registerProcessingTimeTimer(triggerTime);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        System.out.println(ctx.getCurrentKey() + " 在 " + format.format(timestamp) + "触发了定时器的执行");
                    }
                }).print();
        env.execute("ProcessFunctionApp");
    }

    public static class User {
        public int id;
        public String name;
        public int age;

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}