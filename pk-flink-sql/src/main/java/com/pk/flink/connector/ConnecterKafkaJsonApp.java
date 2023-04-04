package com.pk.flink.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ConnecterKafkaJsonApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // {"id":1, "name":"PK", "age":30, "gender":"M", "city":"BJ"}
        /*
         * 字段：
         *      普通字段、物理字段
         *      逻辑字段：根据表达式计算出来的
         *      元数据字段：是针对Kafka中内置的属性值来获得：topic对应的partition、offset、eventtime
         */
        tableEnv.executeSql("CREATE TABLE kafka_flink (\n" +
                "id int,\n" +
                "name string,\n" +
                "age int,\n" +
                "gender string,\n" +
                "city string,\n" +
                //逻辑字段
                "new_age as age+10, \n" +
                //元数据字段
                "event_time TIMESTAMP(3) METADATA FROM 'timestamp', \n" +
                "offs BIGINT METADATA FROM 'offset', \n" +
                "part BIGINT METADATA FROM 'partition' \n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'test05',\n" +
                "'properties.bootstrap.servers' = 'hadoop000:9093,hadoop000:9094,hadoop000:9095',\n" +
                "'properties.group.id' = 'g12',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'json'\n" +
                ")");
        tableEnv.executeSql("desc kafka_flink").print();
        tableEnv.executeSql("select * from kafka_flink").print();
    }
}