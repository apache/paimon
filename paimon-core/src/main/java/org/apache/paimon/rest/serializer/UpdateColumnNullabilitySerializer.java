package org.apache.paimon.rest.serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeJsonParser;
import org.apache.paimon.utils.JsonDeserializer;
import org.apache.paimon.utils.JsonSerializer;

/** Serializer for {@link SchemaChange.UpdateColumnType}. */
public class UpdateColumnTypeSerializer implements JsonSerializer<SchemaChange.UpdateColumnType>, JsonDeserializer<SchemaChange.UpdateColumnType> {

    public static final UpdateColumnTypeSerializer INSTANCE = new UpdateColumnTypeSerializer();

    private static final String FIELD_FILED = "field-names";
    private static final String FIELD_NEW_DATA_TYPE = "new-data-types";
    private static final String FIELD_KEEP_NULLABILITY = "keep-nullability";
    @Override
    public SchemaChange.UpdateColumnType deserialize(JsonNode node) {
        Iterator<JsonNode> fieldJsons =  node.get(FIELD_FILED).elements();
        List<String> fieldNames = new ArrayList<>();
        while (fieldJsons.hasNext()) {
            fieldNames.add(fieldJsons.next().asText());
        }
        DataType dataType = DataTypeJsonParser.parseDataType(node.get(FIELD_NEW_DATA_TYPE));
        boolean keepNullability = node.has(FIELD_KEEP_NULLABILITY) ? node.get(FIELD_KEEP_NULLABILITY).asBoolean() : false;
        return (SchemaChange.UpdateColumnType) SchemaChange.updateColumnType(fieldNames.toArray(new String[0]), dataType, keepNullability);
    }

    @Override
    public void serialize(SchemaChange.UpdateColumnType updateColumnType, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeArrayFieldStart(FIELD_FILED);
        for (String fieldName : updateColumnType.fieldNames()) {
            generator.writeString(fieldName);
        }
        generator.writeEndArray();
        generator.writeObjectField(FIELD_NEW_DATA_TYPE, updateColumnType.newDataType());
        generator.writeBooleanField(FIELD_KEEP_NULLABILITY, updateColumnType.keepNullability());
        generator.writeEndObject();
    }
}
