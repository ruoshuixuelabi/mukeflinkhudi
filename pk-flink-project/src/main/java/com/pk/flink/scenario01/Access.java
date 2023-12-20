package com.pk.flink.scenario01;

import lombok.Data;

@Data
public class Access {
    private int categoryId;  // 类别id
    private String description; // 商品描述
    private int id; // 商品id
    private String ip; // 访问的ip信息
    private float money;
    private String name;
    private String os;
    private int status;
    private long ts;
}