package com.pk.flink.basic;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 以动态传参的方式 传递Flink应用程序所需要的参数
 */
public class PKFlinkSocketWCApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 通过参数传递进来Flink引用程序所需要的参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host");
        int port = tool.getInt("port");

        DataStreamSource<String> source = env.socketTextStream(host, port);
        System.out.println(source.getParallelism());

        // 处理
        source.flatMap((String value, Collector<Tuple2<String,Integer>> out) -> {
            String[] splits = value.split(",");
            for(String split : splits) {
                out.collect(Tuple2.of(split.trim(), 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0) // 这种写法一定要掌握
                .sum(1)
                // 数据输出
                .print();  // 输出到外部系统中去

        env.execute("PKFlinkSocketWCApp--作业名字");
    }
}
