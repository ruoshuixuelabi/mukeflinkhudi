package com.pk.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * 事务型Producer测试
 */
public class KafkaProducerApp03 {
    public static String BROKERS = "hadoop000:9093,hadoop000:9094";
    public static String TOPIC = "pk-2-2";
    KafkaProducer<String, String> kafkaProducer;

    @Before
    public void setUp() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "pk-transcation-10");
        kafkaProducer = new KafkaProducer<>(properties);
    }

    @Test
    public void test01() throws Exception {
        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();
        try {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<String, String>(TOPIC, "pk" + i));
            }
            int a = 1 / 0;
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            kafkaProducer.abortTransaction();
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