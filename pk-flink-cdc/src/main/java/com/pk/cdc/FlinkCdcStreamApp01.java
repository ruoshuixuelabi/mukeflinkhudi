package com.pk.cdc;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class FlinkCdcStreamApp01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("hadoop000")
                .port(13306)
                .scanNewlyAddedTableEnabled(true)
                .databaseList("canal")
                .tableList("canal.user, canal.stu")
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial())
                .deserializer(new PKDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql cdc")
                .print();
        env.execute();
    }
}