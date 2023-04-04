package com.pk.flink.function;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

/**
 * 自定义UDF函数求平均数
 */
public class AvgAggregateFunctionApp {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Table table = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("gender", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())
                ),
                Row.of(1, "M", 80),
                Row.of(2, "M", 100),
                Row.of(3, "F", 99)
        );
        tableEnv.createTemporaryView("t", table);
        tableEnv.createTemporaryFunction("pk_avg", PKAvgFunction.class);
        tableEnv.executeSql("select gender, pk_avg(score) avg_score from t group by gender").print();
    }

    /**
     * 求平均数： 总数  / 个数
     */
    public static class PKAvgFunction extends AggregateFunction<Double, PKAvgAccumulator> {
        @Override
        public Double getValue(PKAvgAccumulator accumulator) {
            return accumulator.sum / accumulator.count;
        }

        @Override
        public PKAvgAccumulator createAccumulator() {
            PKAvgAccumulator accumulator = new PKAvgAccumulator();
            accumulator.count = 0;
            accumulator.sum = 0;
            return accumulator;
        }

        public void accumulate(PKAvgAccumulator acc, Double score) {
            acc.count = acc.count + 1;
            acc.sum = acc.sum + score;
        }
    }

    public static class PKAvgAccumulator {
        public double sum = 0;
        public int count = 0;
    }
}