package com.pk.flink.scenario05.db;//package com.pk.cdc;

import com.google.gson.Gson;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;

public class PKDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Map<String, Object> map = new HashMap<>();
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String table = fields[2];
        map.put("database", database);
        map.put("table", table);
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) { // 我只做了insert  作业：自己去完成update操作
            type = "insert";
        }
        map.put("type", type);
        Struct struct = (Struct) sourceRecord.value();
        Struct after = struct.getStruct("after");
        if (null != after) {
            Schema schema = after.schema();
            Map<String, Object> afterMap = new HashMap<>();
            for (Field field : schema.fields()) {
                afterMap.put(field.name(), after.get(field.name()));
            }
            map.put("data", afterMap);
        }
        Gson gson = new Gson();
        collector.collect(gson.toJson(map));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}