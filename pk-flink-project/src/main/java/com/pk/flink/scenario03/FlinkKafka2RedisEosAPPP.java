package com.pk.flink.scenario03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * 功能需求：使用Flink对接Kafka的数据源，完成词频统计，完成精准一次语义
 *
 */
public class FlinkKafka2RedisEosAPPP {

    public static void main(String[] args) throws Exception {

        DataStream<String> stream = FlinkUtils.createStream(args, SimpleStringSchema.class);
        // TODO... 完成业务逻辑开发

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for (String split : splits) {
                    out.collect(split.trim());
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(x -> x.f0)
                .sum(1);

        res.print();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
        .setHost("hadoop000")  // 指向部署redis的hostname或者是ip
        .setPort(16379)  // 指向的是redis的端口  默认是6379，默认端口在云主机上容易被挖矿
        .setDatabase(8)  // 指向结果写入到redis中的第几个数据库
        .build();

        res.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisExampleMapper()));

        FlinkUtils.environment.execute("EOSApp01");
    }

    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, Integer>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "pk-wc");
        }

        // 从数据中取出key
        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        // 从数据中取出values
        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1 + "";
        }
    }

}
