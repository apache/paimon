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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.Module;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import javax.annotation.Nullable;

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
        OBJECT_MAPPER_INSTANCE.registerModule(new JavaTimeModule());
    }

    public static <V> LinkedHashMap<String, V> parseJsonMap(String jsonString, Class<V> valueType) {
        try {
            LinkedHashMap<String, Object> originalMap =
                    OBJECT_MAPPER_INSTANCE.readValue(
                            jsonString, new TypeReference<LinkedHashMap<String, Object>>() {});
            return originalMap.entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> {
                                        Object value = entry.getValue();
                                        try {
                                            if (!(valueType.isInstance(value))) {
                                                String jsonStr =
                                                        OBJECT_MAPPER_INSTANCE.writeValueAsString(
                                                                value);
                                                return OBJECT_MAPPER_INSTANCE.convertValue(
                                                        jsonStr, valueType);
                                            }
                                            return OBJECT_MAPPER_INSTANCE.convertValue(
                                                    value, valueType);
                                        } catch (JsonProcessingException e) {
                                            throw new RuntimeException(
                                                    "Error converting value to JSON string", e);
                                        }
                                    },
                                    (oldValue, newValue) -> oldValue,
                                    LinkedHashMap::new));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error parsing JSON string", e);
        }
    }

    /**
     * Retrieves a specific node from the given root node and casts it to the specified type.
     *
     * @param <T> The type of the node to be returned.
     * @param root The root node from which the specific node is to be retrieved.
     * @param fieldName The name of the field to retrieve.
     * @param clazz The class of the node to be returned.
     * @return The node cast to the specified type or null if not present.
     * @throws IllegalArgumentException if the node is not of the expected type.
     */
    @Nullable
    public static <T extends JsonNode> T getNodeAs(
            JsonNode root, String fieldName, Class<T> clazz) {
        JsonNode node = root.get(fieldName);
        if (node == null) {
            return null;
        }

        if (clazz.isInstance(node)) {
            return clazz.cast(node);
        }

        throw new IllegalArgumentException(
                String.format(
                        "Expected node '%s' to be of type %s but was %s.",
                        fieldName, clazz.getName(), node.getClass().getName()));
    }

    public static <T> T fromJson(String json, TypeReference<T> typeReference) {
        try {
            return OBJECT_MAPPER_INSTANCE.readValue(json, typeReference);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER_INSTANCE.reader().readValue(json, clazz);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Helper for parsing JSON from a String.
     *
     * @param json a JSON string
     * @param parser a function that converts a JsonNode to a Java object
     * @param <T> type of objects created by the parser
     * @return the parsed Java object
     */
    public static <T> T fromJson(String json, FromJson<T> parser) {
        try {
            return parser.parse(OBJECT_MAPPER_INSTANCE.readValue(json, JsonNode.class));
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

    public static <T> void registerJsonObjects(
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

    /**
     * Parses the provided JSON string and casts it to the specified type of {@link JsonNode}.
     *
     * <p>This method is useful when the exact subtype of {@link JsonNode} is known beforehand and
     * needs to be enforced.
     *
     * @param <T> The type of the JsonNode to return. Must be a subtype of {@link JsonNode}.
     * @param json The JSON string to parse.
     * @param clazz The expected class type of the parsed JSON.
     * @return The parsed JSON as an instance of the specified type.
     * @throws JsonProcessingException If there's an error during JSON parsing.
     * @throws IllegalArgumentException If the parsed JSON is not of the expected type.
     */
    public static <T extends JsonNode> T asSpecificNodeType(String json, Class<T> clazz)
            throws JsonProcessingException {
        JsonNode resultNode = OBJECT_MAPPER_INSTANCE.readTree(json);
        if (!clazz.isInstance(resultNode)) {
            throw new IllegalArgumentException(
                    "Expected node of type "
                            + clazz.getName()
                            + " but was "
                            + resultNode.getClass().getName());
        }
        return clazz.cast(resultNode);
    }

    /** Parses a JSON string and extracts a value of the specified type from the given path keys. */
    public static <T> T extractValueOrDefault(
            JsonNode jsonNode, Class<T> valueType, T defaultValue, String... path)
            throws JsonProcessingException {
        for (String key : path) {
            jsonNode = jsonNode.get(key);
            if (jsonNode == null) {
                if (defaultValue != null) {
                    return defaultValue;
                }
                throw new IllegalArgumentException("Invalid path or key not found: " + key);
            }
        }
        return OBJECT_MAPPER_INSTANCE.treeToValue(jsonNode, valueType);
    }

    /** Parses a JSON string and extracts a value of the specified type from the given path keys. */
    public static <T> T extractValue(JsonNode jsonNode, Class<T> valueType, String... path)
            throws JsonProcessingException {
        return extractValueOrDefault(jsonNode, valueType, null, path);
    }

    /** Checks if a specified node exists in a JSON string. */
    public static boolean isNodeExists(JsonNode jsonNode, String... path) {
        for (String key : path) {
            jsonNode = jsonNode.get(key);
            if (jsonNode == null) {
                return false;
            }
        }
        return true;
    }

    public static <T> T convertValue(Object fromValue, TypeReference<T> toValueTypeRef) {
        return OBJECT_MAPPER_INSTANCE.convertValue(fromValue, toValueTypeRef);
    }

    public static String writeValueAsString(Object value) throws JsonProcessingException {
        return OBJECT_MAPPER_INSTANCE.writer().writeValueAsString(value);
    }

    public static boolean isNull(JsonNode jsonNode) {
        return jsonNode == null || jsonNode.isNull();
    }

    /**
     * A functional interface for parsing JSON data into a specific object type.
     *
     * @param <T> The type of the object to be parsed from JSON.
     */
    @FunctionalInterface
    public interface FromJson<T> {
        T parse(JsonNode node) throws JsonProcessingException;
    }

    private JsonSerdeUtil() {}
}
