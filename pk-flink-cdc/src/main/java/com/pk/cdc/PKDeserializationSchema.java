package com.pk.cdc;

import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class PKDeserializationSchema implements DebeziumDeserializationSchema<String> {
    /**
     * {
     * "db":xxx,
     * "table":xxx,
     * "before":{},
     * "after":{}
     * "op": ""
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db", fields[1]);
        result.put("table", fields[2]);
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (null != before) {
            List<Field> schemaFields = before.schema().fields();
            for (Field field : schemaFields) {
                beforeJson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJson);
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (null != after) {
            List<Field> schemaFields = after.schema().fields();
            for (Field field : schemaFields) {
                afterJson.put(field.name(), after.get(field));
            }
        }
        result.put("after", afterJson);
        //状态
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}