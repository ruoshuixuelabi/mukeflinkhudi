package com.pk.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * WC: 接收socket的数据，进行词频统计
 * <p>
 * Flink State
 * keyedstate
 * operatorstate
 */
public class CustomStateApp01 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("execution.savepoint.path", "file:///Users/rocky/source/flinkworkspace/pk-flink/chk/ac621780bfac274f04e822c6728fec58/chk-4");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/rocky/source/flinkworkspace/pk-flink/chk");
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5));
        // abcdaab
        env.socketTextStream("localhost", 9527)
                .map(new PKStateFunction())
                .print();
        /*
         * 体现出来Flink State管理的牛逼没有
         * 定期的给我们做checkpoint
         * 如果挂了之后
         * 我们只要告诉Flink，你去从哪个位置开始接着进行代码的执行
         * 因为State Flink已经为我们周期性的做了checkpoint道某个地方去了
         */
//        KeyedStateTest(env);
        env.execute();
    }

    private static void KeyedStateTest(StreamExecutionEnvironment env) {
        /*
         * keyedstate来进行编程
         */
        env.socketTextStream("localhost", 9527)
                .keyBy(x -> "0")
                .map(new RichMapFunction<String, String>() {
                    ListState<String> listState;
                    /**
                     * 在open方法中初始化XXXState
                     * 不同state是需要一个xxxxStateDescriptor(名称，类型)
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor("list", String.class)
                        );
                    }
                    @Override
                    public String map(String value) throws Exception {
                        listState.add(value.toLowerCase()); // 更新操作
                        StringBuilder builder = new StringBuilder();
                        for (String s : listState.get()) {
                            builder.append(s);
                        }
                        return builder.toString();
                    }
                }).print();
    }

    public static void test01(StreamExecutionEnvironment env) {
        env.socketTextStream("localhost", 9527)
                .map(String::toLowerCase)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] splits = value.split(",");
                        for (String split : splits) {
                            out.collect(Tuple2.of(split.trim(), 1L));
                        }
                    }
                }).keyBy(x -> x.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                    /**
                     * 算子开始运算前把状态从某个地方读取进来
                     * 文件系统  IO
                     * 数据库  connection
                     * 那么这个地方的数据该是从哪里写入的呢？
                     * 开一个线程，在内部定时的去写入
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                while (true) {
                                    try {
                                        Thread.sleep(10000);
                                        /*
                                         * 判断文件是否存在：不存在就创建
                                         * IO流把cache中的信息通过文件流操作输出到文件中
                                         */
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }).start();
                    }
                    /*
                     * 使用了一个map来模拟了下缓存的作用，state存放在map中
                     * 该map是存放在JVM内存中的
                     * 该JVM重启了，那么你们觉得以前的状态还存在吗？
                     * 咋解决？
                     */
                    Map<String, Long> cache = new HashMap<>();
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                        String word = value.f0;
                        Long cnt = value.f1;
                        // 算子操作前中先去获取到已有的状态
                        Long historyValue = cache.getOrDefault(word, 0L);
                        System.out.println("进来一条数据:" + word + "==>" + cnt);
                        historyValue = historyValue + cnt;
                        // 算子操作后要去更新已有的状态
                        cache.put(word, historyValue);
                        return Tuple2.of(word, historyValue);
                    }
                }).print();
    }
}