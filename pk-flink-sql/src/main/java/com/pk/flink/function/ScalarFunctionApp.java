package com.pk.flink.function;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;

public class ScalarFunctionApp {
    public static void main(String[] args) throws Exception {
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
//        Table table = tableEnv.fromValues(
//                DataTypes.ROW(
//                        DataTypes.FIELD("name", DataTypes.STRING())
//                ),
//                Row.of("pk"),
//                Row.of("zhangsan")
//        );
//        tableEnv.createTemporaryView("t", table);
//        tableEnv.createTemporaryFunction("pk_upper", PKUpperUDF.class);
//        tableEnv.executeSql("select pk_upper(name) from t").print();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
                new Path("data/json/01.json")).build();
        //旧版本写法,现在已经废弃。新版本写法如上面一行
//        DataStreamSource<String> lines = env.readTextFile("data/json/01.json");
        DataStreamSource<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "ss");
        Schema schema = Schema.newBuilder()
                .columnByExpression("line", "f0")
                .build();
        Table table = tableEnv.fromDataStream(lines, schema);
        //旧版本写法,现在已经废弃。新版本写法如上面一行
//        Table table = tableEnv.fromDataStream(lines, $("line"));
        tableEnv.createTemporaryView("clicklog", table);
//        tableEnv.from("clicklog")
//                .select(
//                        call(PKJsonFunction.class, $("line") , "user"),
//                        call(PKJsonFunction.class, $("line") , "url"),
//                        call(PKJsonFunction.class, $("line") , "time")
//                ).execute().print();
        tableEnv.createTemporaryFunction("pk_json", PKJsonFunction.class);
        tableEnv.sqlQuery("select pk_json(line, 'user') as username,pk_json(line, 'url') as url ,pk_json(line, 'time') as ts from clicklog")
                .execute().print();
    }

    public static class PKUpperUDF extends ScalarFunction {
        public String eval(String str) {
            return str.toUpperCase();
        }
    }
}