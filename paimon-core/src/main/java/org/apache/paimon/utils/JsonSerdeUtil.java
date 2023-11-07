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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
     * @return The node cast to the specified type.
     * @throws IllegalArgumentException if the node is not present or if it's not of the expected
     *     type.
     */
    public static <T extends JsonNode> T getNodeAs(
            JsonNode root, String fieldName, Class<T> clazz) {
        return Optional.ofNullable(root)
                .map(r -> r.get(fieldName))
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Expected node '%s' to be of type %s but was either not found or of a different type.",
                                                fieldName, clazz.getName())));
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

    /**
     * Converts the given Java object into its corresponding {@link JsonNode} representation.
     *
     * <p>This method utilizes the Jackson {@link ObjectMapper}'s valueToTree functionality to
     * transform any Java object into a JsonNode, which can be useful for various JSON tree
     * manipulations without serializing the object into a string format first.
     *
     * @param <T> The type of the input object.
     * @param value The Java object to be converted.
     * @return The JsonNode representation of the given object.
     */
    public static <T> JsonNode toTree(T value) {
        return OBJECT_MAPPER_INSTANCE.valueToTree(value);
    }

    /**
     * Adds an array of values to a JSON string under the specified key.
     *
     * @param origin The original JSON string.
     * @param key The key under which the values will be added as an array.
     * @param values A list of values to be added to the JSON string.
     * @return The JSON string with the added array. If the JSON string is not a valid JSON object,
     *     or if the list of values is empty or null, the original JSON string will be returned.
     * @throws RuntimeException If an error occurs while parsing the JSON string or adding the
     *     values.
     */
    public static String putArrayToJsonString(String origin, String key, List<String> values) {
        if (values == null || values.isEmpty()) {
            return origin;
        }

        try {
            JsonNode jsonNode = OBJECT_MAPPER_INSTANCE.readTree(origin);
            if (jsonNode.isObject()) {
                ObjectNode objectNode = (ObjectNode) jsonNode;
                ArrayNode arrayNode = objectNode.putArray(key);
                for (String value : values) {
                    arrayNode.add(value);
                }
                return OBJECT_MAPPER_INSTANCE.writeValueAsString(objectNode);
            } else {
                return origin;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to add array to JSON", e);
        }
    }

    public static boolean isNull(JsonNode jsonNode) {
        return jsonNode == null || jsonNode.isNull();
    }

    private JsonSerdeUtil() {}
}
