package com.pk.flink.sink;

import com.pk.flink.bean.Access;
import com.pk.flink.function.AccessConvertFunction;
import com.pk.flink.function.PKConsoleSinkFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

public class SinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(3000);
//        DataStreamSource<String> source = env.readTextFile("data/access.log");
//        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("data/access.log")).build();
//        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
//        SingleOutputStreamOperator<Access> stream = source.map(new AccessConvertFunction());
        //  num>  ??????????????????1??????????????????num>??????
//        stream.addSink(new PKConsoleSinkFunction());
        //??????StreamingFileSink??????????????????????????? org.apache.flink.connector.file.sink.FileSink
//        StreamingFileSink<String> fileSink = StreamingFileSink
//                .forRowFormat(new Path("out"), new SimpleStringEncoder())
//                .withRollingPolicy(DefaultRollingPolicy.builder() // ?????????????????????????????????
//                        .withRolloverInterval(Duration.ofMinutes(15)) // ?????????????????????
//                        .withInactivityInterval(Duration.ofSeconds(5)) // ??????????????????
//                        .withMaxPartSize(MemorySize.ofMebiBytes(1)) // ???????????????
//                        .build())
//                .build();
//        FileSink<String> fileSink = FileSink
//                .forRowFormat(new Path("out"), new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder() // ?????????????????????????????????
//                                .withRolloverInterval(Duration.ofMinutes(15)) // ?????????????????????
//                                .withInactivityInterval(Duration.ofSeconds(5)) // ??????????????????
//                                .withMaxPartSize(MemorySize.ofMebiBytes(1)) // ???????????????
//                                .build())
//                .build();
////        stream.map(Access::toString).addSink(fileSink);
//        stream.map(Access::toString).sinkTo(fileSink);
        /*
         * ?????????access??????????????????domain?????????????????????????????????traffic?????????????????????redis
         */
//        DataStreamSource<String> source = env.readTextFile("data/access.log");
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("data/access.log")).build();
        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
        SingleOutputStreamOperator<Access> stream = source.map(new AccessConvertFunction());
        SingleOutputStreamOperator<Tuple2<String, Double>> resultStream = stream.map(x -> Tuple2.of(x.getDomain(), x.getTraffic()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(x -> x.f0)
                .sum(1);
//        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
//                .setHost("hadoop000")  // ????????????redis???hostname?????????ip
//                .setPort(16379)  // ????????????redis?????????  ?????????6379?????????????????????????????????????????????
//                .setDatabase(6)  // ?????????????????????redis????????????????????????
//                .build();
//        resultStream.addSink(new RedisSink<Tuple2<String, Double>>(conf, new RedisExampleMapper()));
//        SinkFunction<Tuple2<String, Double>> jdbcSink = JdbcSink.sink(
//                "insert into pk_traffics (domain, traffics) values (?, ?) on duplicate key update traffics=?",
//                (JdbcStatementBuilder<Tuple2<String, Double>>) (pstmt, tuple) -> {
//                    pstmt.setString(1, tuple.f0);
//                    pstmt.setDouble(2, tuple.f1);
//                    pstmt.setDouble(3, tuple.f1);
//                },
//                JdbcExecutionOptions.builder()
//                        .withBatchSize(5)
//                        .withBatchIntervalMs(200)
//                        .withMaxRetries(5)
//                        .build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:mysql://hadoop000:3306/pk_flink")
//                        .withDriverName("com.mysql.jdbc.Driver")
//                        .withUsername("root")
//                        .withPassword("!Ruozedata123")
//                        .build()
//        );
//        resultStream.addSink(jdbcSink);
        stream.map(Access::toString).writeToSocket("localhost", 9528, new SimpleStringSchema());
        env.execute("SinkApp"); // ???????????????Flink???????????????
    }

    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, Double>> {

        /**
         * ????????????redis??????hash????????????
         * pk-traffics
         * key1  value1    ==> pk1.com  ....
         * key2  value2    ==> pk2.com  ....
         * key3  value3    ==> pk3.com  ....
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "pk-traffics");
        }

        // ??????????????????key
        @Override
        public String getKeyFromData(Tuple2<String, Double> data) {
            return data.f0;
        }

        // ??????????????????values
        @Override
        public String getValueFromData(Tuple2<String, Double> data) {
            return data.f1 + "";
        }
    }
}