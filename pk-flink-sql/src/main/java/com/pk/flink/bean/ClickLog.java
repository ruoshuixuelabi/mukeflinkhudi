package com.pk.flink.bean;
import lombok.Data;
@Data
public class ClickLog {
    private String user;
    private String url;
    private String time;
}