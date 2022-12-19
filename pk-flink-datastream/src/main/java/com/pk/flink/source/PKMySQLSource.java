package com.pk.flink.source;

import com.pk.flink.bean.Student;
import com.pk.flink.utils.MySQLUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * RichSourceFunction: Rich  +   SourceFunction
 * Rich: 包含了生命周期方法  open  close
 * SourceFunction：单
 *
 * 自定义二次开发：按照框架(Flink/Spark/....)所提供的接口，去实现自己的业务逻辑即可
 * 自定义Source
 * 自定义Sink
 *
 *
 * 扩展：对于Spark SQL的外部数据源熟悉吗？ 按照Spark所提供的接口，自己实现业务逻辑
 *
 */
public class PKMySQLSource extends RichSourceFunction<Student> {


    Connection connection;
    PreparedStatement pstmt;

    /**
     * 初始化操作，建立connection
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtils.getConnection();
        pstmt = connection.prepareStatement("select * from student");
    }

    /**
     * 释放资源，关闭connection
     */
    @Override
    public void close() throws Exception {
        MySQLUtils.close(pstmt);
        MySQLUtils.close(connection);

    }

    /**
     * 业务逻辑：就是把表中的数据读取出来 ==> Student
     */
    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int age = rs.getInt("age");
            Student student = new Student(id, name, age);
            ctx.collect(student);
        }
    }

    @Override
    public void cancel() {

    }
}
