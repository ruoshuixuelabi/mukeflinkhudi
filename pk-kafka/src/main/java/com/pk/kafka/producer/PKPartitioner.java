package com.pk.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器：
 * <p>
 * Producer发送消息到Kafka时，根据消息的内容进行分区
 * PK  ==>   0
 * RUOZE  ==> 1
 */
public class PKPartitioner implements Partitioner {

    /**
     * @param topic      主题
     * @param key        key
     * @param keyBytes   key序列化后的字节数组
     * @param value      value
     * @param valueBytes value序列化后的字节数组
     * @param cluster
     * @return 返回消息对应的分区编号
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String msg = value.toString();
        int partition;
        if (msg.toLowerCase().contains("pk")) {
            partition = 0;
        } else {
            partition = 1;
        }
        return partition;
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {
    }

    /**
     * 参数设置
     */
    @Override
    public void configure(Map<String, ?> configs) {
    }
}