package com.pk.flink.scenario05.db;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
public class ClickHouseSink extends RichSinkFunction<String> {
    Connection connection;
    PreparedStatement pstmt;
    private Connection getConnection(){
        try{
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            String url = "jdbc:clickhouse://hadoop000:8123/pk";
            connection = DriverManager.getConnection(url, "default","000000");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = getConnection();
        String sql = "insert into pk_product(id,name,catagory,note,price) values(?,?,?,?)";
        pstmt = connection.prepareStatement(sql);
    }
    @Override
    public void invoke(String value, Context context) throws Exception {
        Gson gson = new Gson();
        HashMap<String,Object> map = gson.fromJson(value, HashMap.class);
        String database = (String) map.get("database");
        String table = (String) map.get("table");
        String type = (String) map.get("type");
        if("ruozedata_cdc".equals(database) && "pk_product".equals(table)) {
            if("insert".equals(type)) {
                System.out.println("====insert->" + value);
                LinkedTreeMap<String,Object> data = (LinkedTreeMap<String,Object>)map.get("data");
                Double id = (Double) data.get("id");
                String name = (String)data.get("name");
                String catagory = (String)data.get("catagory");
                String note = (String)data.get("catagory");
                Double price = (Double)data.get("price");
                pstmt.setInt(1, id.intValue());
                pstmt.setString(2, name);
                pstmt.setString(3, catagory);
                pstmt.setString(4, note);
                pstmt.setDouble(5, price);
                pstmt.executeUpdate();
            }
        }
    }
    @Override
    public void close() throws Exception {
        if(pstmt != null) pstmt.close();
        if(connection != null) connection.close();
    }
}