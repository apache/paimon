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

public class AddColumnSerializer implements JsonSerializer<SchemaChange.AddColumn>, JsonDeserializer<SchemaChange.AddColumn> {

    public static final AddColumnSerializer INSTANCE = new AddColumnSerializer();

    private static final String FIELD_FILED_NAME = "field-names";
    private static final String FIELD_DATA_TYPE_NAME = "data-types";
    private static final String FIELD_DESCRIPTION_NAME = "desc";
    private static final String FIELD_MOVE_NAME = "move";
    @Override
    public SchemaChange.AddColumn deserialize(JsonNode node) {
        Iterator<JsonNode> fieldJsons =  node.get(FIELD_FILED_NAME).elements();
        List<String> fieldNames = new ArrayList<>();
        while (fieldJsons.hasNext()) {
            fieldNames.add(fieldJsons.next().asText());
        }
        DataType dataType = DataTypeJsonParser.parseDataType(node.get(FIELD_DATA_TYPE_NAME));
        String description = node.has(FIELD_DESCRIPTION_NAME) ? node.get(FIELD_DESCRIPTION_NAME).asText() : null;
        SchemaChange.Move move = node.has(FIELD_MOVE_NAME) ? MoveSerializer.INSTANCE.deserialize(node.get(FIELD_MOVE_NAME)) : null;
        return (SchemaChange.AddColumn) SchemaChange.addColumn(fieldNames.toArray(new String[0]), dataType, description, move);
    }

    @Override
    public void serialize(SchemaChange.AddColumn addColumn, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeArrayFieldStart(FIELD_FILED_NAME);
        for (String fieldName : addColumn.fieldNames()) {
            generator.writeString(fieldName);
        }
        generator.writeEndArray();
        generator.writeObjectField(FIELD_DATA_TYPE_NAME, addColumn.dataType());
        if(addColumn.description() != null) {
            generator.writeStringField(FIELD_DESCRIPTION_NAME, addColumn.description());
        }
        if(addColumn.move() != null) {
            generator.writeObjectField(FIELD_MOVE_NAME, addColumn.move());
        }
        generator.writeEndObject();
    }
}
