package com.pk.flink.connector;
import org.apache.flink.table.api.*;
public class ConnecterFileSystemCsvApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
//                .column("cities", DataTypes.ARRAY(DataTypes.STRING()))
                .build();
        tableEnv.createTemporaryTable("pk_csv1",
                TableDescriptor.forConnector("filesystem")
                        .schema(schema)
                        .option("path", "data/csv/01.csv")
//                        .option("csv.field-delimiter", "|")
//                        .option("csv.quote-character", "$")
//                        .option("csv.disable-quote-character", "false")
//                        .option("csv.allow-comments", "true")
//                        .option("csv.array-element-delimiter", ":")
                        .format("csv")
                        .build()
        );
        tableEnv.executeSql("desc pk_csv1").print();
        tableEnv.executeSql("select * from pk_csv1").print();
        tableEnv.executeSql("create table pk_csv2(\n" +
                "id int,\n" +
                "name string,\n" +
                "age int,\n" +
                "cities array<string> \n" +
                ") with (\n" +
                "'connector' = 'filesystem',\n" +
                "'path' = 'data/csv/01.csv', \n" +
//                "'csv.field-delimiter' = '|', \n" +
//                "'csv.quote-character' = '$', \n" +
//                "'csv.disable-quote-character' = 'false', \n" +
//                "'csv.allow-comments' = 'true', \n" +
//                "'csv.array-element-delimiter' = ':', \n" +
                "'format' = 'csv'\n" +
                ")");
        tableEnv.executeSql("desc pk_csv2").print();
        tableEnv.executeSql("select * from pk_csv2").print();
    }
}