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

/** Serializer for {@link SchemaChange.UpdateColumnNullability}. */
public class UpdateColumnNullabilitySerializer implements JsonSerializer<SchemaChange.UpdateColumnNullability>, JsonDeserializer<SchemaChange.UpdateColumnNullability> {

    public static final UpdateColumnNullabilitySerializer INSTANCE = new UpdateColumnNullabilitySerializer();

    private static final String FIELD_FILED = "field-names";
    private static final String FIELD_NEW_NULLABILITY = "new-nullability";
    @Override
    public SchemaChange.UpdateColumnNullability deserialize(JsonNode node) {
        Iterator<JsonNode> fieldJsons =  node.get(FIELD_FILED).elements();
        List<String> fieldNames = new ArrayList<>();
        while (fieldJsons.hasNext()) {
            fieldNames.add(fieldJsons.next().asText());
        }
        boolean newNullability = node.get(FIELD_NEW_NULLABILITY).asBoolean();
        return (SchemaChange.UpdateColumnNullability) SchemaChange.updateColumnNullability(fieldNames.toArray(new String[0]), newNullability);
    }

    @Override
    public void serialize(SchemaChange.UpdateColumnNullability updateColumnNullability, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeArrayFieldStart(FIELD_FILED);
        for (String fieldName : updateColumnNullability.fieldNames()) {
            generator.writeString(fieldName);
        }
        generator.writeEndArray();
        generator.writeBooleanField(FIELD_NEW_NULLABILITY, updateColumnNullability.newNullability());
        generator.writeEndObject();
    }
}
