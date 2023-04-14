package com.pk.flink.scenario05.access;

import com.alibaba.fastjson2.JSON;
import com.pk.flink.scenario03.FlinkUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FlinkETLApp {
    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> source = FlinkUtils.createStreamV2(args, PKKafkaRecordDeserializationSchema.class);
        FastDateFormat format = FastDateFormat.getInstance("yyyyMMdd-HH");
        source.map(new MapFunction<Tuple2<String, String>, AccessV2>() {
                    @Override
                    public AccessV2 map(Tuple2<String, String> value) throws Exception {
                        try {
                            AccessV2 bean = JSON.parseObject(value.f1, AccessV2.class);
                            bean.setId(value.f0);
                            long time = bean.getTime();
                            String[] splits = format.format(time).split("-");
                            String day = splits[0];
                            String hour = splits[1];
                            bean.setDay(day);
                            bean.setHour(hour);
                            /*
                             * TODO...
                             * 1) 请根据ip解析出来省份、城市
                             * 2) CH表是一个非分区表，建议做成分区表，以day作为分区
                             * 3) 都是在IDEA运行，你们打个包，将作业运行在服务器  standalone、yarn
                             * 4) offset建议改成最早的
                             * Flink进来的表作为ODS
                             * 后续的表就基于ODS去扩展： dwd  ads
                             * 实时数仓的层级是要小于离线数仓的层级
                             */
                            return bean;
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                    }
                }).filter(x -> x != null)
                .filter(new FilterFunction<AccessV2>() {
                    @Override
                    public boolean filter(AccessV2 value) throws Exception {
                        return "startup".equals(value.getEvent());
                    }
                })
                .addSink(JdbcSink.sink(
                        "insert into ch_event_startup values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (pstmt, x) -> {
                            pstmt.setString(1, x.getId());
                            pstmt.setString(2, x.getDevice());
                            pstmt.setString(3, x.getDeviceType());
                            pstmt.setString(4, x.getOs());
                            pstmt.setString(5, x.getEvent());
                            pstmt.setString(6, x.getNet());
                            pstmt.setString(7, x.getChannel());
                            pstmt.setString(8, x.getUid());
                            pstmt.setInt(9, x.getNu());
                            pstmt.setString(10, x.getIp());
                            pstmt.setLong(11, x.getTime());
                            pstmt.setString(12, x.getVersion());
                            pstmt.setString(13, x.getProvince());
                            pstmt.setString(14, x.getCity());
                            pstmt.setString(15, x.getDay());
                            pstmt.setString(16, x.getHour());
                            pstmt.setLong(17, System.currentTimeMillis());
                        },
                        JdbcExecutionOptions.builder().withBatchSize(50).withBatchIntervalMs(4000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://hadoop000:8123/pk")
                                .withUsername("default")
                                .withPassword("000000")
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .build()
                ));
        // 将数据sink到clickhouse中取
        FlinkUtils.environment.execute();
    }
}