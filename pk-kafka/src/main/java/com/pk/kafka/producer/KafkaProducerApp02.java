package com.pk.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * 分区策略
 */
public class KafkaProducerApp02 {
    public static String BROKERS = "hadoop000:9093,hadoop000:9094";
    public static String TOPIC = "pk-2-2";
    KafkaProducer<String, String> kafkaProducer;

    /**
     * 完成初始化的构造操作
     */
    @Before
    public void setUp() {
        Properties properties = new Properties();
        // 连接到Kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        /*
         * Kafka消息的key和value的序列化方式
         * MR：Writable  DBWritable... WritableComparable
         */
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 如果你想去提升你的Kafka的性能问题，是不是该从如下环节入手
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 3);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.put(ProducerConfig.ACKS_CONFIG, 1);
        // 设置自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PKPartitioner.class.getName());
        kafkaProducer = new KafkaProducer<>(properties);
    }

    /**
     * If a partition is specified in the record, use it
     * <p>
     * If no partition is specified but a key is present choose a partition based on a hash of the key
     * hash(key) % 分区数
     * 思考：适用于什么场景呢？
     * <p>
     * If no partition or key is present choose the sticky partition that changes when the batch is full.
     * 黏性分区器
     */
    @Test
    public void test01() throws Exception {
        for (int i = 0; i < 50; i++) {
            kafkaProducer.send(new ProducerRecord<>(TOPIC, "pk" + i), (metadata, exception) -> {
                if (null == exception) {
                    System.out.println("Topic：" + metadata.topic() + " Partition:" + metadata.partition());
                } else {
                    System.out.println(exception.getMessage());
                }
            });
            Thread.sleep(2);
        }
    }

    /**
     * 自定义分区器的测试
     */
    @Test
    public void test02() throws Exception {
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>(TOPIC, "ruoze" + i), (metadata, exception) -> {
                if (null == exception) {
                    System.out.println("Topic：" + metadata.topic() + " Partition:" + metadata.partition());
                } else {
                    System.out.println(exception.getMessage());
                }
            });
        }
    }

    /**
     * 完成最后的资源释放操作
     */
    @After
    public void tearDown() {
        if (null != kafkaProducer) {
            kafkaProducer.close();
        }
    }
}