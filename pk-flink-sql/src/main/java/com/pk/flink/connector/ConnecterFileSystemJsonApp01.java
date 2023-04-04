package com.pk.flink.connector;

import org.apache.flink.table.api.*;

public class ConnecterFileSystemJsonApp01 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Schema schema = Schema.newBuilder()
                .column("user", DataTypes.STRING())
                .column("time", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                .build();
        tableEnv.createTemporaryTable("pk_json",
                TableDescriptor.forConnector("filesystem")
                        .schema(schema)
                        .option("path", "data/json/01.json")
                        .format("json")
                        .build()
        );
        tableEnv.executeSql("desc pk_json").print();
        tableEnv.executeSql("select * from pk_json").print();
        tableEnv.executeSql("create table pk_json2(\n" +
                "`user` STRING,\n" +
                "`time` string,\n" +
                "url string\n" +
                ") with (\n" +
                "'connector' = 'filesystem',\n" +
                "'path' = 'data/json/01.json', \n" +
                "'format' = 'json'\n" +
                ")");
        tableEnv.executeSql("desc pk_json2").print();
        tableEnv.executeSql("select * from pk_json2").print();
    }
}