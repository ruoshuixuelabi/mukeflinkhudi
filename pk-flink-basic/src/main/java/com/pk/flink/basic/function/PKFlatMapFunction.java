package com.pk.flink.basic.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class PKFlatMapFunction implements FlatMapFunction<String, String> {
    /**
     * @param value The input value. 其实就是step1中读取到的文件内容的一行行的数据
     * @param out   The collector for returning result values.
     * @throws Exception
     */
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] splits = value.split(",");
        for (String split : splits) {
            out.collect(split.toLowerCase().trim());
        }
    }
}