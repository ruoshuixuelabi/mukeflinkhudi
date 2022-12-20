package com.pk.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Kafka Consumer API编程
 * <p>
 * 入口类：
 * <p>
 * <p>
 * <p>
 * https://cwiki.apache.org/confluence/display/KAFKA/Kafka%20Detailed%20Consumer%20Coordinator%20Design
 */
public class KafkaConsumerApp {
    public static final String BROKERS = "hadoop000:9093,hadoop000:9094,hadoop000:9095";
    public static final String TOPIC = "pk-3-1";
    public static final String GROUP = "pk-test-group";
    KafkaConsumer<String, String> kafkaConsumer;

    @Before
    public void setUp() {
        Properties properties = new Properties();
        // 连接到Kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        // 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        // 创建一个消费者对象
        kafkaConsumer = new KafkaConsumer<>(properties);
    }

    @Test
    public void test01() {
        List topics = new ArrayList();
        topics.add(TOPIC);
        kafkaConsumer.subscribe(topics);
        // 消费kafka中topic的数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                String key = record.key();
                String value = record.value();
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                System.out.println("key:" + key + " , value: " + value + " , topic: " + topic + " , partition: " + partition + " , offset: " + offset);
            }
        }
    }

    @Test
    public void test02() {
        List<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition(TOPIC, 1));
        // 手工指定你要消费的topic对应的partition
        kafkaConsumer.assign(topicPartitions);
        // 消费kafka中topic的数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                String key = record.key();
                String value = record.value();
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                System.out.println("key:" + key + " , value: " + value + " , topic: " + topic + " , partition: " + partition + " , offset: " + offset);
            }
        }
    }

    @Test
    public void test03_1() {
        List topics = new ArrayList();
        topics.add(TOPIC);
        kafkaConsumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                String key = record.key();
                String value = record.value();
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                System.out.println("key:" + key + " , value: " + value + " , topic: " + topic + " , partition: " + partition + " , offset: " + offset);
            }
        }
    }

    @Test
    public void test03_2() {
        List topics = new ArrayList();
        topics.add(TOPIC);
        kafkaConsumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                String key = record.key();
                String value = record.value();
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                System.out.println("key:" + key + " , value: " + value + " , topic: " + topic + " , partition: " + partition + " , offset: " + offset);
            }
        }
    }

    @Test
    public void test03_3() {
        List topics = new ArrayList();
        topics.add(TOPIC);
        kafkaConsumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                String key = record.key();
                String value = record.value();
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                System.out.println("key:" + key + " , value: " + value + " , topic: " + topic + " , partition: " + partition + " , offset: " + offset);
            }
        }
    }

    @After
    public void tearDown() {
        if (null != kafkaConsumer) {
            kafkaConsumer.close();
        }
    }
}