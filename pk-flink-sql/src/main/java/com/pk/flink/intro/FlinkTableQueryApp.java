package com.pk.flink.intro;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * Flink Table API&SQL Query
 */
public class FlinkTableQueryApp {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // source
        Table sourceTable = tableEnv.fromValues(
                // 定义数据类型
                DataTypes.ROW(
                        DataTypes.FIELD("user", DataTypes.STRING()),
                        DataTypes.FIELD("time", DataTypes.STRING()),
                        DataTypes.FIELD("url", DataTypes.STRING())
                ),
                // 定义数据
                row("Mary","12:00:00","./home"),
                row("Bob","12:00:00","./cart"),
                row("Mary","12:00:05","./prod?id=1"),
                row("Liz","12:01:00","./home"),
                row("Bob","12:01:30",",/prod?id=3"),
                row("Mary","12:01:40","./prod?id=7")
        );

        // 按照user分组，求每个user访问的次数  select user,count(1) cnt from xx group by user;
//        sourceTable.groupBy($("user"))
//                .select($("user"), $("url").count().as("cnt"))
//                .execute().print();

        /**
         * createTemporaryView(name, Table)
         * createTemporaryTable(name, TableDescriptor)
         */
        tableEnv.createTemporaryView("sourceTable", sourceTable);
        tableEnv.executeSql("desc sourceTable").print();
        tableEnv.executeSql("select user,count(1) cnt from sourceTable group by user").print();
    }
}
