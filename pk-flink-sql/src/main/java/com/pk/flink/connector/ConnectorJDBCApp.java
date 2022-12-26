package com.pk.flink.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ConnectorJDBCApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.executeSql("CREATE TABLE flink_click_log (\n" +
                "`user` STRING,\n" +
                "`time` STRING,\n" +
                "url STRING\n" +
                ") WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://hadoop000:3306/ruozedata',\n" +
                "'table-name' = 'click_log',\n" +
                "'username'='root',\n" +
                "'password'='123456'\n" +
                ");");
        tableEnv.executeSql("desc flink_click_log").print();
        tableEnv.executeSql("select user, count(1) from flink_click_log group by user").print();
        /*
         * Mary | 12:00:00 | ./home      |
         | Bob  | 12:00:00 | ./cart      |
         | Mary | 12:00:05 | ./prod?id=1 |
         | Liz  | 12:01:00 | ./home      |
         | Bob  | 12:01:30 | /prod?id=3  |
         | Mary | 12:01:40 | ./prod?id=7
         */
    }
}
