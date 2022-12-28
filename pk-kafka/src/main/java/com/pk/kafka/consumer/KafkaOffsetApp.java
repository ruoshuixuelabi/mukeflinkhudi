package com.pk.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka关注offset的提交
 * <p>
 * 自动提交offset
 * 1  2  3  4  5
 * 处理到这里时，业务逻辑(写数据库)，挂了
 * <p>
 * 手动提交offset
 * 同步提交：block  重试机制  效率低
 * 异步提交： 不带有重试机制  效率高
 */
public class KafkaOffsetApp {

    public static final String BROKERS = "hadoop000:9093,hadoop000:9094,hadoop000:9095";
    public static final String TOPIC = "pk-offset-2";
    public static final String GROUP = "pk-test-group";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 连接到Kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        // 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        // 自动提交：周期性的自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        // 创建一个消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
            kafkaConsumer.commitSync();  // 同步提交
//            kafkaConsumer.commitAsync();  // 异步提交
            // 1  2  3  4   5
            // ...   挂了
        }
    }
}