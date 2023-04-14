package com.pk.flink.scenario05.db;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基于CDC去接收MySQL实时变化的数据然后输出到CH
 */
public class FlinkCDCClickHouseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("hadoop000")
                .port(13306)
                .scanNewlyAddedTableEnabled(true)
                .databaseList("ruozedata_cdc")
                .tableList("ruozedata_cdc.pk_product")
                .username("root")
                .password("000000")
                .serverTimeZone("UTC")
                .deserializer(new PKDeserializationSchema())
                .build();
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "PK CDC Source");
//        stream.print();
        //  TODO   将stream输出到CH
        stream.addSink(new ClickHouseSink());
        env.execute();
    }
}