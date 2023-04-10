package com.pk.flink.scenario04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LiveApp {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // ...
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 定义Source
        tableEnv.executeSql("CREATE TABLE sourceTable (\n" +
                "ts bigint,\n" +
                "type string,\n" +
                "userid string,\n" +
                "province string, \n" +
                "time_ltz as TO_TIMESTAMP_LTZ(ts,3), \n" +
                "WATERMARK FOR time_ltz as time_ltz - INTERVAL '10' SECOND \n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'pkk',\n" +
                "'properties.bootstrap.servers' = 'hadoop000:9093,hadoop000:9094,hadoop000:9095',\n" +
                "'properties.group.id' = 'testpk',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("desc sourceTable").print();
//        tableEnv.executeSql("select * from sourceTable").print();
        // 定义Sink
        tableEnv.executeSql("CREATE TABLE sinkTable (\n" +
                "`ts` TIMESTAMP(3),\n" +
                "`catagory` string,\n" +
                "`province` string,\n" +
                "`cnt` bigint,\n" +
                "primary key(`ts`, `catagory`, `province`) not enforced \n" +
                ") WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://hadoop000:13306/ruozedata',\n" +
                "'table-name' = 'stat_result',\n" +
                "'username'='root',\n" +
                "'password'='000000'\n" +
                ");");
        // 执行SQL，将源端的数据写入到sink
        tableEnv.executeSql("insert into sinkTable select TUMBLE_START(time_ltz, INTERVAL '1' MINUTE) as ts, type as catagory, province, count(*) as cnt from sourceTable group by type,province,TUMBLE(time_ltz, INTERVAL '1' MINUTE)");
    }
}