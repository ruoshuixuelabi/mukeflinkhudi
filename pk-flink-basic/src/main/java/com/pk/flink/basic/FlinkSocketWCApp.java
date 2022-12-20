package com.pk.flink.basic;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用Flink对接Socket的数据并进行词频统计
 * <p>
 * 大数据处理的三段论： 输入  处理  输出
 * <p>
 * <p>
 * 思考：Flink WebUI  8081
 * 本地运行的端口是多少呢？
 */
public class FlinkSocketWCApp {
    public static void main(String[] args) throws Exception {
        /*
         * 获取Flink执行的环境
         * getExecutionEnvironment() 这是我们用的最多的一种
         * createLocalEnvironment()  这种仅限于本地开发使用
         * createRemoteEnvironment()  知道就行，开发不用
         */
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        /*
         * 数据源：可以通过多种不同的数据源接入数据：socket  kafka  text
         * 官网上描述的是 env.addSource(...)
         * socket的方式对应的并行度是1，因为它来自于SourceFunction的实现
         */
        DataStreamSource<String> source = env.socketTextStream("hadoop000", 9527);
        System.out.println(source.getParallelism());
        // 处理
        source.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        out.collect(Tuple2.of(split.trim(), 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0) // 这种写法一定要掌握
                .sum(1)
                // 数据输出
                .print();  // 输出到外部系统中去
        env.execute("作业名字");
    }
}