package com.pk.flink.function;

import org.apache.flink.api.common.functions.Partitioner;

public class PKPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        System.out.println(numPartitions);
        if ("pk1.com".equals(key)) {
            return 0;
        } else if ("pk2.com".equals(key)) {
            return 1;
        } else {
            return 2;
        }
    }
}