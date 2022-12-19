package com.pk.flink.ts;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class TimeAttributesApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE TABLE pk_wm_01 (\n" +
                "id int,\n" +
                "ts bigint,\n" +
                "pt as PROCTIME(),\n" +
                "rt as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "WATERMARK FOR rt AS rt - INTERVAL '2' SECOND\n" +
                ") WITH (\n" +
                "'connector' = 'filesystem',\n" +
                "'path' = 'data/wm.json',\n" +
                "'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("desc pk_wm_01").print();
        tableEnv.executeSql("select id,ts,rt,CURRENT_WATERMARK(rt) wm from pk_wm_01").print();

    }
}
