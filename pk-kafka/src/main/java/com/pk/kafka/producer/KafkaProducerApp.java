package com.pk.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Kafka 生产者 API 案例开发
 * <p>
 * 代码作为消息生产的载体，将消息发送到topic中去，使用命令行消费终端进行消息的消费输出
 * <p>
 * <p>
 * 代码 =msg=> topic ==> consumer
 * <p>
 * 记住入口：KafkaProducer
 */
public class KafkaProducerApp {
    public static String BROKERS = "hadoop000:9093,hadoop000:9094";
    public static String TOPIC = "pk-1-1";
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
        kafkaProducer = new KafkaProducer<>(properties);
    }

    /**
     * Asynchronously：有两种方式
     * 同步
     */
    @Test
    public void test01() {
        for (int i = 20; i < 25; i++) {
            kafkaProducer.send(new ProducerRecord<>(TOPIC, "pk" + i));
        }
    }

    /**
     * 带回调函数的异步发送方式
     */
    @Test
    public void test02() {
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>(TOPIC, "pk" + i), new Callback() {
                /**
                 * 如果exception为null，消息发送成功
                 * 如果exception不为null，消息发送失败
                 * @param metadata 消息的元数据信息
                 * @param exception 异常
                 */
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null == exception) {
                        System.out.println("Topic：" + metadata.topic() + " Partition:" + metadata.partition());
                    } else {
                        System.out.println(exception.getMessage());
                    }
                }
            });
        }
    }

    /**
     * 同步的方式发送消息
     * <p>
     * send方法会返回一个Future对象
     * <p>
     * 阻塞
     * <p>
     * 同步 vs 异步
     */
    @Test
    public void test03() throws Exception {
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>(TOPIC, "pk" + i)).get();
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