package com.pk.flink.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * TODO 该程序目前一直报错不知道原因
 */
public class TableFunctionApp {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Table table = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("digits", DataTypes.STRING())
                ),
                Row.of(1, "zs", "AA,A,AAA"),
                Row.of(2, "ls", "888,66")
        );
        tableEnv.createTemporaryView("t", table);
        tableEnv.createTemporaryFunction("pk_split", PKSplitFunction.class);
        tableEnv.executeSql("select * from t ,LATERAL TABLE(pk_split(digits,','))").print();
    }

    @FunctionHint(output = @DataTypeHint("ROW<digit STRING, length INT>"))
    public static class PKSplitFunction extends TableFunction<Row> {
        public void eval(String str, String delimiter) {
            for (String s : str.split(delimiter)) {
                collect(Row.of(s, s.length()));
            }
        }
    }
}