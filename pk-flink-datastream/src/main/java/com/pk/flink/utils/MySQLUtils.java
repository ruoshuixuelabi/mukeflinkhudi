package com.pk.flink.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLUtils {

    public static Connection getConnection() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection("jdbc:mysql://hadoop000:3306/pk_flink", "root", "!Ruozedata123");
    }

    public static void close(AutoCloseable closeable) {
        if (null != closeable) {
            try {
                closeable.close(); // null.close
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                closeable = null;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(getConnection());
    }
}