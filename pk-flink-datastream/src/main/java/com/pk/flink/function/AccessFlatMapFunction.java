package com.pk.flink.function;

import com.pk.flink.bean.Access;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class AccessFlatMapFunction extends RichFlatMapFunction<Access, String> {
    @Override
    public void flatMap(Access value, Collector<String> out) throws Exception {
        if (value.getDomain().equals("pk1.com")) { // 域名为pk1.com
            out.collect(value.getDomain()); // 一个元素 ==> 1
        } else { // 域名为非pk1.com
            out.collect("--domain--" + value.getDomain());
            out.collect("--traffic--" + value.getTraffic());
        }
    }
}