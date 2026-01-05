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

import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Contains;
import org.apache.paimon.predicate.EndsWith;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Like;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.NotIn;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.StartsWith;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.TransformPredicate;
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
import java.util.Objects;
import java.util.stream.Collectors;

/** SerDe for {@link Predicate} JSON entries. */
public class PredicateJsonSerde {

    private static final ObjectMapper MAPPER = RESTApi.OBJECT_MAPPER;

    private static final String FIELD_PREDICATE = "predicate";

    private static final String FIELD_TYPE = "type";
    private static final String TYPE_COMPOUND = "compound";
    private static final String TYPE_TRANSFORM = "transform";

    private static final String FIELD_FUNCTION = "function";
    private static final String FIELD_CHILDREN = "children";

    private static final String FIELD_TRANSFORM = "transform";
    private static final String FIELD_LITERALS = "literals";

    private PredicateJsonSerde() {}

    @Nullable
    public static Predicate parse(@Nullable String json) throws JsonProcessingException {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }

        JsonNode root = MAPPER.readTree(json);
        JsonNode predicateNode = root.get(FIELD_PREDICATE);
        if (predicateNode == null || predicateNode.isNull()) {
            return null;
        }
        return parsePredicateNode(predicateNode);
    }

    public static String toJsonString(Predicate predicate) {
        return toJsonString(toJsonNode(predicate));
    }

    private static String toJsonString(JsonNode predicateObject) {
        ObjectNode root = MAPPER.createObjectNode();
        root.set(FIELD_PREDICATE, predicateObject);
        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize predicate entry json.", e);
        }
    }

    public static ObjectNode compoundPredicateNode(
            String function, List<? extends JsonNode> children) {
        ObjectNode predicate = MAPPER.createObjectNode();
        predicate.put(FIELD_TYPE, TYPE_COMPOUND);
        predicate.put(FIELD_FUNCTION, function);
        ArrayNode array = MAPPER.createArrayNode();
        if (children != null) {
            for (JsonNode child : children) {
                array.add(child);
            }
        }
        predicate.set(FIELD_CHILDREN, array);
        return predicate;
    }

    private static JsonNode toJsonNode(Predicate predicate) {
        if (predicate instanceof LeafPredicate) {
            LeafPredicate leaf = (LeafPredicate) predicate;
            Transform transform =
                    new FieldTransform(new FieldRef(leaf.index(), leaf.fieldName(), leaf.type()));
            return transformPredicateJsonNode(
                    transform, leafFunctionName(leaf.function()), leaf.literals());
        }

        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) predicate;
            List<JsonNode> children =
                    compound.children().stream()
                            .map(PredicateJsonSerde::toJsonNode)
                            .collect(Collectors.toList());
            String fn = compound.function().equals(Or.INSTANCE) ? "OR" : "AND";
            return compoundPredicateNode(fn, children);
        }

        if (predicate instanceof TransformPredicate) {
            TransformPredicate transformPredicate = (TransformPredicate) predicate;
            return transformPredicateJsonNode(
                    transformPredicate.transform(),
                    leafFunctionName(transformPredicate.function()),
                    transformPredicate.literals());
        }

        throw new IllegalArgumentException(
                "Unsupported predicate type: " + predicate.getClass().getName());
    }

    private static ObjectNode transformPredicateJsonNode(
            Transform transform, String function, @Nullable List<Object> literals) {
        ObjectNode predicate = MAPPER.createObjectNode();
        predicate.put(FIELD_TYPE, TYPE_TRANSFORM);
        predicate.set(FIELD_TRANSFORM, TransformJsonSerde.toJsonNode(transform));
        predicate.put(FIELD_FUNCTION, function);

        ArrayNode lits = MAPPER.createArrayNode();
        if (literals != null) {
            for (Object lit : literals) {
                lits.add(MAPPER.valueToTree(lit));
            }
        }
        predicate.set(FIELD_LITERALS, lits);
        return predicate;
    }

    private static String leafFunctionName(LeafFunction function) {
        if (function.equals(Equal.INSTANCE)) {
            return "EQUAL";
        } else if (function.equals(NotEqual.INSTANCE)) {
            return "NOT_EQUAL";
        } else if (function.equals(GreaterThan.INSTANCE)) {
            return "GREATER_THAN";
        } else if (function.equals(GreaterOrEqual.INSTANCE)) {
            return "GREATER_OR_EQUAL";
        } else if (function.equals(LessThan.INSTANCE)) {
            return "LESS_THAN";
        } else if (function.equals(LessOrEqual.INSTANCE)) {
            return "LESS_OR_EQUAL";
        } else if (function.equals(In.INSTANCE)) {
            return "IN";
        } else if (function.equals(NotIn.INSTANCE)) {
            return "NOT_IN";
        } else if (function.equals(IsNull.INSTANCE)) {
            return "IS_NULL";
        } else if (function.equals(IsNotNull.INSTANCE)) {
            return "IS_NOT_NULL";
        } else if (function.equals(StartsWith.INSTANCE)) {
            return "STARTS_WITH";
        } else if (function.equals(EndsWith.INSTANCE)) {
            return "ENDS_WITH";
        } else if (function.equals(Contains.INSTANCE)) {
            return "CONTAINS";
        } else if (function.equals(Like.INSTANCE)) {
            return "LIKE";
        }

        throw new IllegalArgumentException(
                "Unsupported leaf function: " + function.getClass().getName());
    }

    private static Predicate parsePredicateNode(JsonNode node) {
        String type = requiredText(node, FIELD_TYPE);
        if (TYPE_COMPOUND.equals(type)) {
            String fnText = requiredText(node, FIELD_FUNCTION);
            CompoundPredicate.Function fn = "OR".equals(fnText) ? Or.INSTANCE : And.INSTANCE;

            JsonNode childrenNode = node.get(FIELD_CHILDREN);
            List<Predicate> children =
                    childrenNode == null || childrenNode.isNull()
                            ? new ArrayList<>()
                            : toList(childrenNode).stream()
                                    .map(PredicateJsonSerde::parsePredicateNode)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList());
            return new CompoundPredicate(fn, children);
        }

        if (TYPE_TRANSFORM.equals(type)) {
            Transform transform =
                    TransformJsonSerde.parseTransformNode(required(node, FIELD_TRANSFORM));
            LeafFunction fn = parseLeafFunction(requiredText(node, FIELD_FUNCTION));

            List<Object> literals = new ArrayList<>();
            JsonNode literalsNode = node.get(FIELD_LITERALS);
            if (literalsNode instanceof ArrayNode) {
                DataType literalType = transform.outputType();
                for (JsonNode lit : (ArrayNode) literalsNode) {
                    Object javaObj = MAPPER.convertValue(lit, Object.class);
                    literals.add(PredicateBuilder.convertJavaObject(literalType, javaObj));
                }
            }

            return TransformPredicate.of(transform, fn, literals);
        }

        throw new IllegalArgumentException("Unsupported predicate type: " + type);
    }

    private static LeafFunction parseLeafFunction(String function) {
        switch (function) {
            case "EQUAL":
                return Equal.INSTANCE;
            case "NOT_EQUAL":
                return NotEqual.INSTANCE;
            case "GREATER_THAN":
                return GreaterThan.INSTANCE;
            case "GREATER_OR_EQUAL":
                return GreaterOrEqual.INSTANCE;
            case "LESS_THAN":
                return LessThan.INSTANCE;
            case "LESS_OR_EQUAL":
                return LessOrEqual.INSTANCE;
            case "IN":
                return In.INSTANCE;
            case "NOT_IN":
                return NotIn.INSTANCE;
            case "IS_NULL":
                return IsNull.INSTANCE;
            case "IS_NOT_NULL":
                return IsNotNull.INSTANCE;
            case "STARTS_WITH":
                return StartsWith.INSTANCE;
            case "ENDS_WITH":
                return EndsWith.INSTANCE;
            case "CONTAINS":
                return Contains.INSTANCE;
            case "LIKE":
                return Like.INSTANCE;
            default:
                throw new IllegalArgumentException("Unsupported leaf function: " + function);
        }
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

    private static List<JsonNode> toList(JsonNode node) {
        List<JsonNode> list = new ArrayList<>();
        if (node instanceof ArrayNode) {
            node.forEach(list::add);
        }
        return list;
    }
}
