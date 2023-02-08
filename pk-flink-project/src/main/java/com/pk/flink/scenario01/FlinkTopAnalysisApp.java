package com.pk.flink.scenario01;

import com.alibaba.fastjson2.JSON;
import com.pk.flink.utils.DateUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class FlinkTopAnalysisApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // step1: 接入要处理的数据源
//        DataStreamSource<String> source = env.readTextFile("data/productaccess.log");
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("data/productaccess.log")).build();
        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
        // step2: 使用Flink Transformation算子进行各种维度的统计分析
        /*
         * 接入的数据是json格式 ==> Access
         */
        SingleOutputStreamOperator<Tuple3<String, String, Long>> resultStream = source
                .map(json -> JSON.parseObject(json, Access.class))
                .map(x -> Tuple3.of(x.getName(), DateUtils.ts2Date(x.getTs(), "yyyyMMdd"), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .keyBy(x -> x.f0)
                .sum(2);
        // step3: 将结果输出到目的地
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("172.18.30.88")  // 指向部署redis的hostname或者是ip
                .setPort(6379)  // 指向的是redis的端口  默认是6379，默认端口在云主机上容易被挖矿
                .setDatabase(6)  // 指向结果写入到redis中的第几个数据库
                .build();
        resultStream.addSink(new RedisSink<Tuple3<String, String, Long>>(conf, new RedisExampleMapper()));
        env.execute("FlinkTopAnalysisApp");
    }

    public static class RedisExampleMapper implements RedisMapper<Tuple3<String, String, Long>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "pk-product-access");
        }

        @Override
        public String getKeyFromData(Tuple3<String, String, Long> value) {
            return value.f0;
        }

        @Override
        public String getValueFromData(Tuple3<String, String, Long> value) {
            return value.f2.toString();
        }
    }
}