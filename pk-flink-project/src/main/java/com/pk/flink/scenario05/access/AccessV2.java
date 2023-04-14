package com.pk.flink.scenario05.access;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AccessV2 {
    private String id;
    private String device;
    private String deviceType;
    private String os;
    private String event;
    private String net;
    private String channel;
    private String uid;
    private int nu;
    private String ip;
    private long time;
    private String version;
    private String province;
    private String city;
    private String day;
    private String hour;
}
