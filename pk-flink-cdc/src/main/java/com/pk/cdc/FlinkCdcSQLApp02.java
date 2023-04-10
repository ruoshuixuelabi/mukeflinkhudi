package com.pk.cdc;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
public class FlinkCdcSQLApp02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE flink_user (\n" +
                "     id STRING,\n" +
                "     name STRING,\n" +
                "     address STRING,\n" +
                "     PRIMARY KEY(id) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'hadoop000',\n" +
                "     'port' = '13306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = '000000',\n" +
                "     'scan.startup.mode' = 'initial',\n" +
                "     'database-name' = 'canal',\n" +
                "     'table-name' = 'user');");
        tableEnv.executeSql("select * from flink_user").print();
    }
}