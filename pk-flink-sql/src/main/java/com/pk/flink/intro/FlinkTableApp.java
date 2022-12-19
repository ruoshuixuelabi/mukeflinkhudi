package com.pk.flink.intro;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.row;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table API编程范式
 */
public class FlinkTableApp {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // source
        Table sourceTable = tableEnv.fromValues(
                // 定义数据类型
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),

                // 定义数据
                row(1, "PK哥"),
                row(2, "张三")
        );

        tableEnv.createTemporaryView("sourceTable", sourceTable);

        sourceTable.printSchema();

        // transformation
        Table resultTable = tableEnv.from("sourceTable")
                .select($("id"), $("name"));


        // sink
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .build();
        tableEnv.createTemporaryTable("sinkTable",
                TableDescriptor.forConnector("print")
                    .schema(schema).build());

        resultTable.executeInsert("sinkTable");
    }
}
