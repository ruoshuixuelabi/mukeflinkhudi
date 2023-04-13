package com.pk.clickhouse;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 通过Flink处理后将数据写入到ClickHouse中
 * <p>
 * <li>1）创建clickhouse表
 * <li>2）使用Flink进行业务逻辑处理
 * <li>3）addSink(clickhouseSink)
 * <p>
 * clickhouseSink: 就是一个普通的flink-connector-jdbc
 * JdbcSink.sink(
 * sqlDmlStatement,                       // mandatory
 * jdbcStatementBuilder,                  // mandatory
 * jdbcExecutionOptions,                  // optional
 * jdbcConnectionOptions                  // mandatory
 * );
 */
public class FlinkClickHouseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SinkFunction<User> clickhouseSink = JdbcSink.sink(
                "insert into pk_users values (?,?,?)",
                new JdbcStatementBuilder<User>() {
                    @Override
                    public void accept(PreparedStatement pstmt, User user) throws SQLException {
                        pstmt.setInt(1, user.getId());
                        pstmt.setString(2, user.getName());
                        pstmt.setInt(3, user.getAge());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(200)
                        .withBatchSize(10)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://hadoop000:8123/pk")
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .build()
        );
        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, User>() {
                    @Override
                    public User map(String value) throws Exception {
                        String[] splits = value.split(",");
                        return new User(Integer.parseInt(splits[0].trim()), splits[1].trim(), Integer.parseInt(splits[2].trim()));
                    }
                }).addSink(clickhouseSink);
        env.execute("FlinkClickHouseApp");
    }
}