package com.pk.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Access {
    /**
     * 定义时间
     */
    private long time;
    /**
     * 域名
     */
    private String domain;
    /**
     * 流量
     */
    private double traffic;
}