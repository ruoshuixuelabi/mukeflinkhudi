package com.pk.clickhouse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
/**
 * 基于JDBC的方式访问ClickHouse
 */
public class ClickHouseJDBCApp {
    public static void main(String[] args)throws Exception {
//        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://hadoop000:8123/pk";
        Connection connection = DriverManager.getConnection(url);
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from emp");
        while(rs.next()) {
            int empno = rs.getInt("empno");
            String ename = rs.getString("ename");
            System.out.println(empno + "==>" + ename);
        }
        rs.close();
        stmt.close();
        connection.close();
    }
}