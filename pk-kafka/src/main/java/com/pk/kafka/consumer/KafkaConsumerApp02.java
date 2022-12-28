package com.pk.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

/**
 * Kafka 分区&rebalance策略验证
 * <p>
 * Range策略  7分区   3消费者   7/3=2  剩1
 * C0: 0 1 2
 * C1: 3 4
 * C2: 5 6
 * <p>
 * <p>
 * 入口类：
 * <p>
 * kafka-console-consumer.sh --topic __consumer_offsets --bootstrap-server hadoop000:9093,hadoop000:9094,hadoop000:9095 --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" --consumer.config config/consumer.properties --from-beginning
 */
public class KafkaConsumerApp02 {
    public static final String BROKERS = "hadoop000:9093,hadoop000:9094,hadoop000:9095";
    public static final String TOPIC = "pk-7-1";
    public static final String GROUP = "pk-test-group";
    KafkaConsumer<String, String> kafkaConsumer;

    @BeforeEach
    public void setUp() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        kafkaConsumer = new KafkaConsumer<>(properties);
    }

    @Test
    public void test03_1() {
        kafkaConsumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("取消分区：" + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("被分配新的：" + partitions);
            }
        });
        while (true) {
            kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        }
    }

    @Test
    public void test03_2() {
        kafkaConsumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("取消分区：" + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("被分配新的：" + partitions);
            }
        });
        while (true) {
            kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        }
    }

    @Test
    public void test03_3() {
        kafkaConsumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("取消分区：" + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("被分配新的：" + partitions);
            }
        });
        while (true) {
            kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        }
    }

    @AfterEach
    public void tearDown() {
        if (null != kafkaConsumer) {
            kafkaConsumer.close();
        }
    }
}