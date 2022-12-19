package com.pk.flink.intro;

import com.pk.flink.bean.ClickLog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;


/**
 * DataStream <==> Table
 */
public class FlinkTableWithDataStreamApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        SingleOutputStreamOperator<ClickLog> clickLogStream = env.fromElements(
                "Mary,12:00:00,./home",
                "Bob,12:00:00,./cart",
                "Mary,12:00:05,./prod?id=1",
                "Liz,12:01:00,./home",
                "Bob,12:01:30,/prod?id=3",
                "Mary,12:01:40,./prod?id=7"
        ).map(x -> {
            String[] splits = x.split(",");
            ClickLog clickLog = new ClickLog();
            clickLog.setUser(splits[0].trim());
            clickLog.setTime(splits[1].trim());
            clickLog.setUrl(splits[2].trim());
            return clickLog;
        });

        // 将DataStream ==> Table
        Table table = tableEnv.fromDataStream(clickLogStream);

        Table resultTable = table
                .where($("user").isEqual("Mary"))
                .select($("user"), $("url"), $("time"));

        resultTable.printSchema();
        resultTable.execute().print();

//        // 从Table ==> DataStream
//        DataStream<ClickLog> clicklogs = tableEnv.toDataStream(resultTable, ClickLog.class);
//
//        clicklogs.print();

        env.execute();
    }
}
