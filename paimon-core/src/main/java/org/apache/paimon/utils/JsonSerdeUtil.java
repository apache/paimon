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

package org.apache.paimon.utils;

import org.apache.paimon.schema.SchemaSerializer;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeJsonParser;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.Module;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** A utility class that provide abilities for JSON serialization and deserialization. */
public class JsonSerdeUtil {

    /**
     * Object mapper shared instance to serialize and deserialize the plan. Note that creating and
     * copying of object mappers is expensive and should be avoided.
     */
    private static final ObjectMapper OBJECT_MAPPER_INSTANCE;

    static {
        OBJECT_MAPPER_INSTANCE = new ObjectMapper();
        OBJECT_MAPPER_INSTANCE.registerModule(createPaimonJacksonModule());
    }

    private static final TypeReference<LinkedHashMap<String, ?>> GENERIC_MAP_TYPE =
            new TypeReference<LinkedHashMap<String, ?>>() {};

    public static <V> LinkedHashMap<String, V> parseJsonMap(String jsonString, Class<V> valueType) {
        try {
            LinkedHashMap<String, Object> originalMap =
                    OBJECT_MAPPER_INSTANCE.readValue(
                            jsonString, new TypeReference<LinkedHashMap<String, Object>>() {});
            return originalMap.entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry ->
                                            OBJECT_MAPPER_INSTANCE.convertValue(
                                                    entry.getValue(), valueType),
                                    (oldValue, newValue) -> oldValue,
                                    LinkedHashMap::new));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error parsing JSON string", e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER_INSTANCE.reader().readValue(json, clazz);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> String toJson(T t) {
        try {
            return OBJECT_MAPPER_INSTANCE.writerWithDefaultPrettyPrinter().writeValueAsString(t);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> String toFlatJson(T t) {
        try {
            return OBJECT_MAPPER_INSTANCE.writer().writeValueAsString(t);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Module createPaimonJacksonModule() {
        SimpleModule module = new SimpleModule("Paimon");
        registerJsonObjects(
                module, TableSchema.class, SchemaSerializer.INSTANCE, SchemaSerializer.INSTANCE);
        registerJsonObjects(
                module,
                DataField.class,
                DataField::serializeJson,
                DataTypeJsonParser::parseDataField);
        registerJsonObjects(
                module, DataType.class, DataType::serializeJson, DataTypeJsonParser::parseDataType);
        return module;
    }

    private static <T> void registerJsonObjects(
            SimpleModule module,
            Class<T> clazz,
            JsonSerializer<T> serializer,
            JsonDeserializer<T> deserializer) {
        module.addSerializer(
                new StdSerializer<T>(clazz) {
                    @Override
                    public void serialize(T t, JsonGenerator generator, SerializerProvider provider)
                            throws IOException {
                        serializer.serialize(t, generator);
                    }
                });
        module.addDeserializer(
                clazz,
                new StdDeserializer<T>(clazz) {
                    @Override
                    public T deserialize(JsonParser parser, DeserializationContext context)
                            throws IOException {
                        return deserializer.deserialize(parser.readValueAsTree());
                    }
                });
    }

    private JsonSerdeUtil() {}
}
