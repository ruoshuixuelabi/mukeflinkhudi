package com.pk.flink.scenario05.access;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
public class PKKafkaRecordDeserializationSchema implements KafkaRecordDeserializationSchema<Tuple2<String,String>> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Tuple2<String, String>> out) throws IOException {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String id = topic+"_"+partition+"_"+offset;
        String value = new String(record.value(), StandardCharsets.UTF_8);
        out.collect(Tuple2.of(id, value));
    }
    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        });
    }
}