package org.apache.paimon.rest.serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.utils.JsonDeserializer;
import org.apache.paimon.utils.JsonSerializer;

/** Serializer for {@link SchemaChange.RenameColumn}. */
public class RenameColumnSerializer implements JsonSerializer<SchemaChange.RenameColumn>, JsonDeserializer<SchemaChange.RenameColumn> {

    public static final RenameColumnSerializer INSTANCE = new RenameColumnSerializer();

    private static final String FIELD_FILED_NAME = "field-names";
    private static final String FIELD_NEW_NAME = "new-name";
    @Override
    public SchemaChange.RenameColumn deserialize(JsonNode node) {
        Iterator<JsonNode> fieldJsons =  node.get(FIELD_FILED_NAME).elements();
        List<String> fieldNames = new ArrayList<>();
        while (fieldJsons.hasNext()) {
            fieldNames.add(fieldJsons.next().asText());
        }
        String newName = node.has(FIELD_NEW_NAME) ? node.get(FIELD_NEW_NAME).asText() : null;
        return (SchemaChange.RenameColumn) SchemaChange.renameColumn(fieldNames.toArray(new String[0]), newName);
    }

    @Override
    public void serialize(SchemaChange.RenameColumn renameColumn, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeArrayFieldStart(FIELD_FILED_NAME);
        for (String fieldName : renameColumn.fieldNames()) {
            generator.writeString(fieldName);
        }
        generator.writeEndArray();
        if(renameColumn.newName() != null) {
            generator.writeStringField(FIELD_NEW_NAME, renameColumn.newName());
        }
        generator.writeEndObject();
    }
}
