/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.rest.serializer;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeJsonParser;
import org.apache.paimon.utils.JsonDeserializer;
import org.apache.paimon.utils.JsonSerializer;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Serializer for the {@link Schema} class to support RESTCatalog. */
public class SchemaSerializer implements JsonSerializer<Schema>, JsonDeserializer<Schema> {

    public static final SchemaSerializer INSTANCE = new SchemaSerializer();

    private static final String FIELD_FILED_NAME = "fields";
    private static final String FIELD_PARTITION_KEYS_NAME = "partitionKeys";
    private static final String FIELD_PRIMARY_KEYS_NAME = "primaryKeys";
    private static final String FIELD_OPTIONS_NAME = "options";
    private static final String FIELD_COMMENT_NAME = "comment";

    @Override
    public void serialize(Schema schema, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeArrayFieldStart(FIELD_FILED_NAME);
        for (DataField field : schema.fields()) {
            field.serializeJson(generator);
        }
        generator.writeEndArray();
        generator.writeArrayFieldStart(FIELD_PARTITION_KEYS_NAME);
        for (String partitionKey : schema.partitionKeys()) {
            generator.writeString(partitionKey);
        }
        generator.writeEndArray();
        generator.writeArrayFieldStart(FIELD_PRIMARY_KEYS_NAME);
        for (String partitionKey : schema.primaryKeys()) {
            generator.writeString(partitionKey);
        }
        generator.writeEndArray();
        generator.writeObjectFieldStart(FIELD_OPTIONS_NAME);
        for (Map.Entry<String, String> entry : schema.options().entrySet()) {
            generator.writeStringField(entry.getKey(), entry.getValue());
        }
        generator.writeEndObject();
        if (schema.comment() != null) {
            generator.writeStringField(FIELD_COMMENT_NAME, schema.comment());
        }
        generator.writeEndObject();
    }

    @Override
    public Schema deserialize(JsonNode node) {
        Iterator<JsonNode> fieldJsons = node.get(FIELD_FILED_NAME).elements();
        List<DataField> fields = new ArrayList<>();
        while (fieldJsons.hasNext()) {
            fields.add(DataTypeJsonParser.parseDataField(fieldJsons.next()));
        }
        Iterator<JsonNode> partitionJsons = node.get(FIELD_PARTITION_KEYS_NAME).elements();
        List<String> partitionKeys = new ArrayList<>();
        while (partitionJsons.hasNext()) {
            partitionKeys.add(partitionJsons.next().asText());
        }

        Iterator<JsonNode> primaryJsons = node.get(FIELD_PRIMARY_KEYS_NAME).elements();
        List<String> primaryKeys = new ArrayList<>();
        while (primaryJsons.hasNext()) {
            primaryKeys.add(primaryJsons.next().asText());
        }
        JsonNode optionsJson = node.get(FIELD_OPTIONS_NAME);
        Map<String, String> options = new HashMap<>();
        Iterator<String> optionsKeys = optionsJson.fieldNames();
        while (optionsKeys.hasNext()) {
            String key = optionsKeys.next();
            options.put(key, optionsJson.get(key).asText());
        }
        JsonNode commentNode = node.get(FIELD_COMMENT_NAME);
        String comment = null;
        if (commentNode != null) {
            comment = commentNode.asText();
        }
        return new Schema(fields, partitionKeys, primaryKeys, options, comment);
    }
}
