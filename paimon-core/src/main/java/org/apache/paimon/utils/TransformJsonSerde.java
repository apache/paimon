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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.CastTransform;
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.ConcatWsTransform;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.HashMaskTransform;
import org.apache.paimon.predicate.NullTransform;
import org.apache.paimon.predicate.PartialMaskTransform;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.UpperTransform;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** SerDe for {@link Transform} JSON entries. */
public class TransformJsonSerde {

    private static final ObjectMapper MAPPER = RESTApi.OBJECT_MAPPER;

    private static final String FIELD_TRANSFORM = "transform";
    private static final String FIELD_TYPE = "type";

    private static final String TRANSFORM_TYPE_FIELD = "field";
    private static final String TRANSFORM_TYPE_UPPER = "upper";
    private static final String TRANSFORM_TYPE_CONCAT = "concat";
    private static final String TRANSFORM_TYPE_CONCAT_WS = "concat_ws";
    private static final String TRANSFORM_TYPE_CAST = "cast";
    private static final String TRANSFORM_TYPE_MASK = "mask";
    private static final String TRANSFORM_TYPE_HASH = "hash";
    private static final String TRANSFORM_TYPE_NULL = "null";
    private static final String TRANSFORM_TYPE_LITERAL = "literal";

    private static final String FIELD_INPUTS = "inputs";
    private static final String FIELD_VALUE = "value";
    private static final String FIELD_TO_DATA_TYPE = "toDataType";
    private static final String FIELD_FIELD = "field";
    private static final String FIELD_PREFIX_LEN = "prefixLen";
    private static final String FIELD_SUFFIX_LEN = "suffixLen";
    private static final String FIELD_MASK = "mask";
    private static final String FIELD_ALGORITHM = "algorithm";
    private static final String FIELD_SALT = "salt";
    private static final String FIELD_INDEX = "index";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_DATA_TYPE = "dataType";

    private TransformJsonSerde() {}

    @Nullable
    public static Transform parse(@Nullable String json) throws JsonProcessingException {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }

        JsonNode root = MAPPER.readTree(json);

        if (root.hasNonNull(FIELD_TRANSFORM) && !root.has(FIELD_TYPE)) {
            return parseTransformNode(required(root, FIELD_TRANSFORM));
        }

        if (root.hasNonNull(FIELD_TYPE)) {
            return parseTransformNode(root);
        }

        throw new IllegalArgumentException("Invalid transform json: missing required fields.");
    }

    public static String toJsonString(Transform transform) {
        ObjectNode root = MAPPER.createObjectNode();
        root.set(FIELD_TRANSFORM, toJsonNode(transform));
        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize transform json.", e);
        }
    }

    public static ObjectNode toJsonNode(Transform transform) {
        if (transform instanceof FieldTransform) {
            return fieldRefToJsonNode(((FieldTransform) transform).fieldRef());
        }

        if (transform instanceof UpperTransform) {
            return stringTransformToJsonNode(TRANSFORM_TYPE_UPPER, transform.inputs());
        }
        if (transform instanceof ConcatTransform) {
            return stringTransformToJsonNode(TRANSFORM_TYPE_CONCAT, transform.inputs());
        }
        if (transform instanceof ConcatWsTransform) {
            return stringTransformToJsonNode(TRANSFORM_TYPE_CONCAT_WS, transform.inputs());
        }

        if (transform instanceof CastTransform) {
            ObjectNode node = MAPPER.createObjectNode();
            node.put(FIELD_TYPE, TRANSFORM_TYPE_CAST);
            FieldRef fieldRef = (FieldRef) transform.inputs().get(0);
            node.set(FIELD_FIELD, fieldRefToJsonNode(fieldRef));
            node.set(FIELD_TO_DATA_TYPE, MAPPER.valueToTree(transform.outputType()));
            return node;
        }

        if (transform instanceof PartialMaskTransform) {
            PartialMaskTransform maskTransform = (PartialMaskTransform) transform;
            ObjectNode node = MAPPER.createObjectNode();
            node.put(FIELD_TYPE, TRANSFORM_TYPE_MASK);
            node.set(FIELD_FIELD, fieldRefToJsonNode(maskTransform.fieldRef()));
            node.put(FIELD_PREFIX_LEN, maskTransform.prefixLen());
            node.put(FIELD_SUFFIX_LEN, maskTransform.suffixLen());
            node.put(FIELD_MASK, maskTransform.mask().toString());
            return node;
        }

        if (transform instanceof HashMaskTransform) {
            HashMaskTransform hashTransform = (HashMaskTransform) transform;
            ObjectNode node = MAPPER.createObjectNode();
            node.put(FIELD_TYPE, TRANSFORM_TYPE_HASH);
            node.set(FIELD_FIELD, fieldRefToJsonNode(hashTransform.fieldRef()));
            node.put(FIELD_ALGORITHM, hashTransform.algorithm());
            BinaryString salt = hashTransform.salt();
            if (salt != null) {
                node.put(FIELD_SALT, salt.toString());
            }
            return node;
        }

        if (transform instanceof NullTransform) {
            NullTransform nullTransform = (NullTransform) transform;
            ObjectNode node = MAPPER.createObjectNode();
            node.put(FIELD_TYPE, TRANSFORM_TYPE_NULL);
            node.set(FIELD_FIELD, fieldRefToJsonNode(nullTransform.fieldRef()));
            return node;
        }

        throw new IllegalArgumentException(
                "Unsupported transform type: " + transform.getClass().getName());
    }

    public static Transform parseTransformNode(JsonNode node) {
        String type = requiredText(node, FIELD_TYPE);
        if (TRANSFORM_TYPE_FIELD.equals(type)) {
            int index = required(node, FIELD_INDEX).asInt();
            String name = requiredText(node, FIELD_NAME);
            DataType dataType =
                    MAPPER.convertValue(required(node, FIELD_DATA_TYPE), DataType.class);
            return new FieldTransform(new FieldRef(index, name, dataType));
        }
        if (TRANSFORM_TYPE_UPPER.equals(type)
                || TRANSFORM_TYPE_CONCAT.equals(type)
                || TRANSFORM_TYPE_CONCAT_WS.equals(type)) {
            List<Object> inputs = parseTransformInputs(required(node, FIELD_INPUTS));
            if (TRANSFORM_TYPE_UPPER.equals(type)) {
                return new UpperTransform(inputs);
            } else if (TRANSFORM_TYPE_CONCAT_WS.equals(type)) {
                return new ConcatWsTransform(inputs);
            } else {
                return new ConcatTransform(inputs);
            }
        }
        if (TRANSFORM_TYPE_CAST.equals(type)) {
            FieldRef fieldRef = parseFieldRef(required(node, FIELD_FIELD));
            DataType toType =
                    MAPPER.convertValue(required(node, FIELD_TO_DATA_TYPE), DataType.class);
            return CastTransform.tryCreate(fieldRef, toType)
                    .orElseThrow(
                            () ->
                                    new IllegalArgumentException(
                                            "Unsupported CAST transform from "
                                                    + fieldRef.type()
                                                    + " to "
                                                    + toType));
        }
        if (TRANSFORM_TYPE_MASK.equals(type)) {
            FieldRef fieldRef = parseFieldRef(required(node, FIELD_FIELD));
            int prefixLen = optionalInt(node, FIELD_PREFIX_LEN, 0);
            int suffixLen = optionalInt(node, FIELD_SUFFIX_LEN, 0);
            String mask = optionalText(node, FIELD_MASK, "*");
            return new PartialMaskTransform(
                    fieldRef, prefixLen, suffixLen, BinaryString.fromString(mask));
        }
        if (TRANSFORM_TYPE_HASH.equals(type)) {
            FieldRef fieldRef = parseFieldRef(required(node, FIELD_FIELD));
            String algorithm = optionalText(node, FIELD_ALGORITHM, "SHA-256");
            String salt = optionalText(node, FIELD_SALT, null);
            return new HashMaskTransform(
                    fieldRef, algorithm, salt == null ? null : BinaryString.fromString(salt));
        }
        if (TRANSFORM_TYPE_NULL.equals(type)) {
            FieldRef fieldRef = parseFieldRef(required(node, FIELD_FIELD));
            return new NullTransform(fieldRef);
        }
        throw new IllegalArgumentException("Unsupported transform type: " + type);
    }

    private static ObjectNode fieldRefToJsonNode(FieldRef fieldRef) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put(FIELD_TYPE, TRANSFORM_TYPE_FIELD);
        node.put(FIELD_INDEX, fieldRef.index());
        node.put(FIELD_NAME, fieldRef.name());
        node.set(FIELD_DATA_TYPE, MAPPER.valueToTree(fieldRef.type()));
        return node;
    }

    private static ObjectNode stringTransformToJsonNode(String type, List<Object> inputs) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put(FIELD_TYPE, type);
        ArrayNode inputNodes = MAPPER.createArrayNode();
        for (Object input : inputs) {
            if (input == null) {
                inputNodes.addNull();
            } else if (input instanceof FieldRef) {
                inputNodes.add(fieldRefToJsonNode((FieldRef) input));
            } else if (input instanceof BinaryString) {
                ObjectNode literal = MAPPER.createObjectNode();
                literal.put(FIELD_TYPE, TRANSFORM_TYPE_LITERAL);
                literal.put(FIELD_VALUE, ((BinaryString) input).toString());
                inputNodes.add(literal);
            } else {
                throw new IllegalArgumentException(
                        "Unsupported transform input type: " + input.getClass().getName());
            }
        }
        node.set(FIELD_INPUTS, inputNodes);
        return node;
    }

    private static List<Object> parseTransformInputs(JsonNode node) {
        List<Object> inputs = new ArrayList<>();
        if (!(node instanceof ArrayNode)) {
            throw new IllegalArgumentException("Transform inputs must be an array.");
        }
        for (JsonNode inputNode : (ArrayNode) node) {
            if (inputNode == null || inputNode.isNull()) {
                inputs.add(null);
                continue;
            }
            if (inputNode.isTextual()) {
                inputs.add(BinaryString.fromString(inputNode.asText()));
                continue;
            }
            String type = requiredText(inputNode, FIELD_TYPE);
            if (TRANSFORM_TYPE_FIELD.equals(type)) {
                inputs.add(parseFieldRef(inputNode));
            } else if (TRANSFORM_TYPE_LITERAL.equals(type)) {
                inputs.add(BinaryString.fromString(requiredText(inputNode, FIELD_VALUE)));
            } else {
                throw new IllegalArgumentException("Unsupported transform input type: " + type);
            }
        }
        return inputs;
    }

    private static FieldRef parseFieldRef(JsonNode node) {
        int index = required(node, FIELD_INDEX).asInt();
        String name = requiredText(node, FIELD_NAME);
        DataType dataType = MAPPER.convertValue(required(node, FIELD_DATA_TYPE), DataType.class);
        return new FieldRef(index, name, dataType);
    }

    private static JsonNode required(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return v;
    }

    private static String requiredText(JsonNode node, String field) {
        return required(node, field).asText();
    }

    private static int optionalInt(JsonNode node, String field, int defaultValue) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            return defaultValue;
        }
        return v.asInt();
    }

    private static String optionalText(JsonNode node, String field, String defaultValue) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            return defaultValue;
        }
        return v.asText();
    }
}
