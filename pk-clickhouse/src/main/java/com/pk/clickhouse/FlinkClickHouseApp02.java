package com.pk.clickhouse;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 通过Flink读取ClickHouse中的数据
 */
public class FlinkClickHouseApp02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickHouseSource()).print();
        env.execute("FlinkClickHouseApp");
    }
}

class ClickHouseSource implements SourceFunction<User> {
    @Override
    public void run(SourceContext<User> ctx) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://hadoop000:8123/pk";
        Connection connection = DriverManager.getConnection(url);
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from pk_users");
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int age = rs.getInt("age");
            ctx.collect(new User(id, name, age));
        }
    }

    @Override
    public void cancel() {
    }
}