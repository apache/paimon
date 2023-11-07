package org.apache.paimon.flink.widetable.msg.cdc.mysql;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.util.List;

public class MysqlCDCCustomerDeserialization implements DebeziumDeserializationSchema<String> {
    private static String DATABASE = "db";
    private static String TABLE = "table";
    public static String SNAPSHOT = "READ";

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector)
            throws Exception {

        // 1.创建 JSON 对象用于存储最终数据
        JSONObject result = new JSONObject();

        Struct value = (Struct) sourceRecord.value();

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        Long es = value.getInt64(Envelope.FieldName.TIMESTAMP);
        if (source != null) {
            Schema sourceSchema = source.schema();
            Field db = sourceSchema.field(DATABASE);
            Object database = source.get(db);

            Field tb = sourceSchema.field(TABLE);
            Object table = source.get(tb);

            result.put("database", database);
            result.put("table", table);
            result.put("es", es + "");
        }

        // 3.获取"before"数据
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);

        JSONObject beforeJson = null;
        if (before != null) {
            beforeJson = new JSONObject();
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (Field field : beforeFields) {
                Object beforeValue = before.get(field);
                String val = null;
                if (beforeValue != null) {
                    val = beforeValue.toString();
                }

                beforeJson.put(field.name(), val);
            }
        }
        result.put(Envelope.FieldName.BEFORE, beforeJson);

        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        JSONObject afterJson = null;
        if (after != null) {
            afterJson = new JSONObject();
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {
                Object afterValue = after.get(field);
                String val = null;
                if (afterValue != null) {
                    val = afterValue.toString();
                }
                afterJson.put(field.name(), val);
            }
        }
        result.put(Envelope.FieldName.AFTER, afterJson);

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString();
        String mode = "UPDATE";
        switch (operation) {
            case Operation.READ:
                type = "INSERT";
                mode = SNAPSHOT;
                break;
            case Operation.UPDATE:
                break;
            case Operation.CREATE:
                type = "INSERT";
                break;
            case Operation.DELETE:
                break;
            case Operation.TRUNCATE:
                type = Envelope.Operation.DELETE.toString();
                break;
        }

        result.put("mode", mode);
        result.put("type", type);
        result.put("ts", System.currentTimeMillis() + "");

        // 7.输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
