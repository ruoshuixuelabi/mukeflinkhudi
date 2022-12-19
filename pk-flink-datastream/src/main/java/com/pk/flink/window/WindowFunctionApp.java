package com.pk.flink.window;

import com.pk.flink.bean.Access;
import com.pk.flink.source.AccessSourceV2;
import com.pk.flink.window.function.PKAvgAggregateFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.mutable.HashSet;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class WindowFunctionApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        test04(env);

        env.execute();
    }

    /**
     * 一行一个数字  1 2 3 4 5
     * 要求：让你们一定要使用keyBy  (1, 数字)
     * 5秒一个滚动窗口   按照相同的key求和  不允许你使用sum
     *
     */
    public static void test01(StreamExecutionEnvironment env) {

        env.socketTextStream("localhost", 9527)
                .map(x -> Tuple2.of(1, Integer.parseInt(x.trim())))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) throws Exception {
                        System.out.println("执行reduce操作：" + x + " , " + y);
                        return Tuple2.of(x.f0, x.f1+y.f1);
                    }
                }).print();
    }


    /**
     * 求平均数
     * a,100
     * a,2
     *
     * 51.0
     */
    public static void test02(StreamExecutionEnvironment env) {

        env.socketTextStream("localhost", 9527)
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String value) throws Exception {
                    String[] splits = value.split(",");
                    return Tuple2.of(splits[0].trim(), Long.parseLong(splits[1].trim()));
                }
            }).keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new PKAvgAggregateFunction())
                .print();

        /**
         * 需求：求平均数
         *
         * 平均数 = 总和 / 个数
         *
         * 那么为了求出平均数，我们必然是需要先算出 value的和 以及 次数
         *
         * AggregateFunction<T, ACC, R>
         */
    }


    /**
     * 使用ProcessWindowFunction完成窗口内数据的排序，并输出
     *
     * 计数窗口   5条数据一个窗口
     *
     * 全量
     *
     * 一种是apply  一种process
     */
    public static void test03(StreamExecutionEnvironment env) {

        env.socketTextStream("localhost", 9527)
                .map(x -> Integer.parseInt(x.trim()))
                .countWindowAll(5)
                .apply(new AllWindowFunction<Integer, Integer, GlobalWindow>() {
                    @Override
                    public void apply(GlobalWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception {
                        List<Integer> list = new ArrayList<>();
                        for (Integer value : values) {
                            list.add(value);
                        }

                        list.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return o2 - o1;
                            }
                        });

                        for (Integer res : list) {
                            out.collect(res);
                        }
                    }
                }).print().setParallelism(1);
    }


    /**
     * 全量 配合 增量一起使用
     *
     * 需求：求UV（使用增量函数去求）  输出一个统计信息（全量输出）
     */
    public static void test04(StreamExecutionEnvironment env) {

        DataStreamSource<Access> source = env.addSource(new AccessSourceV2());
        source.print("----原始数据---");
        source.keyBy(x -> true)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(
                        // 自定义增量聚合函数
                        new AggregateFunction<Access, HashSet<String>, Long>() {
                            @Override
                            public HashSet<String> createAccumulator() {
                                return new HashSet<String>();
                            }

                            @Override
                            public HashSet<String> add(Access value, HashSet<String> accumulator) {

                                System.out.println("---add invoked....---");
                                accumulator.add(value.getDomain());
                                return accumulator;
                            }

                            @Override
                            public Long getResult(HashSet<String> accumulator) {
                                return (long) accumulator.size();
                            }

                            @Override
                            public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
                                return null;
                            }
                        }
                        ,

                        // 自定义窗口处理函数，输出最终信息
                        new ProcessWindowFunction<Long, String, Boolean, TimeWindow>() {
                            @Override
                            public void process(Boolean aBoolean, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();

                                StringBuilder res = new StringBuilder();
                                res.append("窗口:【")
                                        .append(new Timestamp(start))
                                        .append(",")
                                        .append(new Timestamp(end))
                                        .append("】，UV是：")
                                        .append(elements.iterator().next());

                                out.collect(res.toString());
                            }
                        }
                ).print();
    }
}
