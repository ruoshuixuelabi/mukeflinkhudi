package com.pk.kafka.utils;

import com.pk.kafka.consumer.KafkaConsumerApp;
import com.pk.kafka.consumer.KafkaConsumerApp02;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 模拟Kafka生产者生产数据
 */
public class KafkaMockData {

    public static void main(String[] args) throws Exception {
        sendMsg(KafkaConsumerApp02.BROKERS, KafkaConsumerApp02.TOPIC, 500);
//        sendMsgWithPartition(KafkaConsumerApp.BROKERS, KafkaConsumerApp.TOPIC, 0,10);
    }

    public static void sendMsg(String brokers, String topic, long count) throws Exception {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        for (long i = 0; i < count; i++) {
            kafkaProducer.send(new ProducerRecord<>(topic, "PK" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(null == exception) {
                        System.out.println("主题：" + metadata.topic() + " Partition:" + metadata.partition());
                    } else {
                        System.out.println(exception.getMessage());
                    }
                }
            });

            Thread.sleep(10);
        }

        kafkaProducer.close();

    }

    public static void sendMsgWithPartition(String brokers, String topic, int partition, long count) throws Exception {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        for (long i = 0; i < count; i++) {
            kafkaProducer.send(new ProducerRecord<>(topic, partition, "", "PK" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(null == exception) {
                        System.out.println("主题：" + metadata.topic() + " Partition:" + metadata.partition());
                    } else {
                        System.out.println(exception.getMessage());
                    }
                }
            });

            Thread.sleep(10);
        }

        kafkaProducer.close();

    }
}
