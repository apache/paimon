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

package org.apache.paimon.predicate;

import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.JsonDeserializer;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.JsonSerializer;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** JSON serde for {@link Predicate} used by REST API. */
public class PredicateJsonSerde implements JsonSerializer<Predicate>, JsonDeserializer<Predicate> {

    public static final PredicateJsonSerde INSTANCE = new PredicateJsonSerde();

    private static final String FIELD_KIND = "kind";

    private static final String KIND_LEAF = "LEAF";
    private static final String KIND_COMPOUND = "COMPOUND";
    private static final String KIND_TRANSFORM = "TRANSFORM";

    private static final String FIELD_FUNCTION = "function";
    private static final String FIELD_LITERALS = "literals";
    private static final String FIELD_CHILDREN = "children";
    private static final String FIELD_OP = "op";
    private static final String FIELD_TRANSFORM = "transform";
    private static final String FIELD_FIELD_REF = "fieldRef";
    private static final String FIELD_INPUTS = "inputs";
    private static final String FIELD_INPUT_KIND = "inputKind";
    private static final String INPUT_KIND_FIELD_REF = "FIELD_REF";
    private static final String INPUT_KIND_LITERAL = "LITERAL";
    private static final String FIELD_VALUE = "value";
    private static final String FIELD_TYPE = "type";

    private static final String TRANSFORM_FIELD = "FIELD";
    private static final String TRANSFORM_CAST = "CAST";
    private static final String TRANSFORM_UPPER = "UPPER";
    private static final String TRANSFORM_CONCAT = "CONCAT";
    private static final String TRANSFORM_CONCAT_WS = "CONCAT_WS";

    private final Map<String, LeafFunction> functionByName = new HashMap<>();

    private PredicateJsonSerde() {
        register(Equal.INSTANCE);
        register(NotEqual.INSTANCE);
        register(LessThan.INSTANCE);
        register(LessOrEqual.INSTANCE);
        register(GreaterThan.INSTANCE);
        register(GreaterOrEqual.INSTANCE);
        register(In.INSTANCE);
        register(NotIn.INSTANCE);
        register(IsNull.INSTANCE);
        register(IsNotNull.INSTANCE);
        register(Like.INSTANCE);
        register(StartsWith.INSTANCE);
        register(EndsWith.INSTANCE);
        register(Contains.INSTANCE);
    }

    private void register(LeafFunction function) {
        functionByName.put(function.getClass().getSimpleName(), function);
    }

    @Override
    public void serialize(Predicate predicate, JsonGenerator generator) throws IOException {
        writePredicate(predicate, generator);
    }

    private void writePredicate(Predicate predicate, JsonGenerator g) throws IOException {
        g.writeStartObject();
        if (predicate instanceof LeafPredicate) {
            LeafPredicate leaf = (LeafPredicate) predicate;
            g.writeStringField(FIELD_KIND, KIND_LEAF);
            g.writeStringField(FIELD_FUNCTION, leaf.function().getClass().getSimpleName());
            writeFieldRef(leaf.fieldRef(), g);
            writeLiterals(leaf.type(), leaf.literals(), g);
        } else if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) predicate;
            g.writeStringField(FIELD_KIND, KIND_COMPOUND);
            g.writeStringField(FIELD_OP, compound.function() instanceof And ? "AND" : "OR");
            g.writeFieldName(FIELD_CHILDREN);
            g.writeStartArray();
            for (Predicate child : compound.children()) {
                writePredicate(child, g);
            }
            g.writeEndArray();
        } else if (predicate instanceof TransformPredicate) {
            TransformPredicate tp = (TransformPredicate) predicate;
            g.writeStringField(FIELD_KIND, KIND_TRANSFORM);
            g.writeStringField(FIELD_FUNCTION, tp.function.getClass().getSimpleName());
            g.writeFieldName(FIELD_TRANSFORM);
            writeTransform(tp.transform(), g);
            writeLiterals(tp.transform().outputType(), tp.literals, g);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported predicate: " + predicate.getClass());
        }
        g.writeEndObject();
    }

    private void writeFieldRef(FieldRef fieldRef, JsonGenerator g) throws IOException {
        g.writeFieldName(FIELD_FIELD_REF);
        g.writeStartObject();
        g.writeNumberField("index", fieldRef.index());
        g.writeStringField("name", fieldRef.name());
        g.writeFieldName(FIELD_TYPE);
        // DataType has paimon jackson module support
        g.writeObject(fieldRef.type());
        g.writeEndObject();
    }

    private void writeTransform(Transform transform, JsonGenerator g) throws IOException {
        g.writeStartObject();
        if (transform instanceof FieldTransform) {
            FieldTransform ft = (FieldTransform) transform;
            g.writeStringField(FIELD_KIND, TRANSFORM_FIELD);
            writeFieldRef(ft.fieldRef(), g);
        } else if (transform instanceof CastTransform) {
            CastTransform ct = (CastTransform) transform;
            g.writeStringField(FIELD_KIND, TRANSFORM_CAST);
            writeFieldRef((FieldRef) ct.inputs().get(0), g);
            g.writeObjectField(FIELD_TYPE, ct.outputType());
        } else if (transform instanceof UpperTransform) {
            g.writeStringField(FIELD_KIND, TRANSFORM_UPPER);
            writeTransformInputs(transform.inputs(), g);
        } else if (transform instanceof ConcatTransform) {
            g.writeStringField(FIELD_KIND, TRANSFORM_CONCAT);
            writeTransformInputs(transform.inputs(), g);
        } else if (transform instanceof ConcatWsTransform) {
            g.writeStringField(FIELD_KIND, TRANSFORM_CONCAT_WS);
            writeTransformInputs(transform.inputs(), g);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported transform: " + transform.getClass());
        }
        g.writeEndObject();
    }

    private void writeTransformInputs(List<Object> inputs, JsonGenerator g) throws IOException {
        g.writeFieldName(FIELD_INPUTS);
        g.writeStartArray();
        for (Object input : inputs) {
            g.writeStartObject();
            if (input instanceof FieldRef) {
                g.writeStringField(FIELD_INPUT_KIND, INPUT_KIND_FIELD_REF);
                writeFieldRef((FieldRef) input, g);
            } else {
                g.writeStringField(FIELD_INPUT_KIND, INPUT_KIND_LITERAL);
                if (input == null) {
                    g.writeNullField(FIELD_VALUE);
                } else if (input instanceof Number) {
                    g.writeObjectField(FIELD_VALUE, input);
                } else if (input instanceof Boolean) {
                    g.writeObjectField(FIELD_VALUE, input);
                } else {
                    g.writeStringField(FIELD_VALUE, input.toString());
                }
            }
            g.writeEndObject();
        }
        g.writeEndArray();
    }

    private void writeLiterals(DataType type, List<Object> literals, JsonGenerator g)
            throws IOException {
        g.writeFieldName(FIELD_LITERALS);
        g.writeStartArray();
        if (literals != null) {
            for (Object lit : literals) {
                if (lit == null) {
                    g.writeNull();
                } else if (lit instanceof Number || lit instanceof Boolean) {
                    g.writeObject(lit);
                } else {
                    // For REST stability: write as string for non-primitive literals.
                    g.writeString(lit.toString());
                }
            }
        }
        g.writeEndArray();
    }

    @Override
    public Predicate deserialize(JsonNode node) {
        String kind = node.get(FIELD_KIND).asText();
        switch (kind) {
            case KIND_LEAF:
                return readLeaf(node);
            case KIND_COMPOUND:
                return readCompound(node);
            case KIND_TRANSFORM:
                return readTransformPredicate(node);
            default:
                throw new IllegalArgumentException("Unknown predicate kind: " + kind);
        }
    }

    private Predicate readLeaf(JsonNode node) {
        String functionName = node.get(FIELD_FUNCTION).asText();
        LeafFunction function = lookupFunction(functionName);
        FieldRef fieldRef = readFieldRef(node.get(FIELD_FIELD_REF));
        List<Object> literals = readLiterals(fieldRef.type(), node.get(FIELD_LITERALS));
        return new LeafPredicate(
                function, fieldRef.type(), fieldRef.index(), fieldRef.name(), literals);
    }

    private Predicate readCompound(JsonNode node) {
        String op = node.get(FIELD_OP).asText();
        CompoundPredicate.Function function =
                "AND".equalsIgnoreCase(op) ? And.INSTANCE : Or.INSTANCE;
        JsonNode childrenNode = node.get(FIELD_CHILDREN);
        List<Predicate> children = new ArrayList<>(childrenNode.size());
        for (JsonNode child : childrenNode) {
            children.add(deserialize(child));
        }
        return new CompoundPredicate(function, children);
    }

    private Predicate readTransformPredicate(JsonNode node) {
        String functionName = node.get(FIELD_FUNCTION).asText();
        LeafFunction function = lookupFunction(functionName);
        Transform transform = readTransform(node.get(FIELD_TRANSFORM));
        List<Object> literals = readLiterals(transform.outputType(), node.get(FIELD_LITERALS));
        return TransformPredicate.of(transform, function, literals);
    }

    private FieldRef readFieldRef(JsonNode node) {
        int index = node.get("index").asInt();
        String name = node.get("name").asText();
        DataType type =
                JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.convertValue(
                        node.get(FIELD_TYPE), DataType.class);
        return new FieldRef(index, name, type);
    }

    private Transform readTransform(JsonNode node) {
        String kind = node.get(FIELD_KIND).asText();
        switch (kind) {
            case TRANSFORM_FIELD:
                FieldRef fieldRef = readFieldRef(node.get(FIELD_FIELD_REF));
                return new FieldTransform(fieldRef);
            case TRANSFORM_CAST:
                FieldRef castInput = readFieldRef(node.get(FIELD_FIELD_REF));
                DataType castType =
                        JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.convertValue(
                                node.get(FIELD_TYPE), DataType.class);
                return CastTransform.of(castInput, castType);
            case TRANSFORM_UPPER:
                return new UpperTransform(readTransformInputs(node));
            case TRANSFORM_CONCAT:
                return new ConcatTransform(readTransformInputs(node));
            case TRANSFORM_CONCAT_WS:
                return new ConcatWsTransform(readTransformInputs(node));
            default:
                throw new IllegalArgumentException("Unknown transform kind: " + kind);
        }
    }

    private List<Object> readTransformInputs(JsonNode node) {
        JsonNode inputsNode = node.get(FIELD_INPUTS);
        List<Object> inputs = new ArrayList<>(inputsNode.size());
        for (JsonNode in : inputsNode) {
            String kind = in.get(FIELD_INPUT_KIND).asText();
            if (INPUT_KIND_FIELD_REF.equals(kind)) {
                inputs.add(readFieldRef(in.get(FIELD_FIELD_REF)));
            } else if (INPUT_KIND_LITERAL.equals(kind)) {
                JsonNode valueNode = in.get(FIELD_VALUE);
                if (valueNode == null || valueNode.isNull()) {
                    inputs.add(null);
                } else if (valueNode.isNumber()) {
                    inputs.add(valueNode.numberValue());
                } else if (valueNode.isBoolean()) {
                    inputs.add(valueNode.booleanValue());
                } else {
                    inputs.add(valueNode.asText());
                }
            } else {
                throw new IllegalArgumentException("Unknown transform input kind: " + kind);
            }
        }
        return inputs;
    }

    private List<Object> readLiterals(DataType type, JsonNode node) {
        List<Object> literals = new ArrayList<>(node.size());
        for (JsonNode lit : node) {
            if (lit == null || lit.isNull()) {
                literals.add(null);
            } else if (lit.isNumber()) {
                literals.add(lit.numberValue());
            } else if (lit.isBoolean()) {
                literals.add(lit.booleanValue());
            } else {
                literals.add(lit.asText());
            }
        }
        return literals;
    }

    private LeafFunction lookupFunction(String functionName) {
        LeafFunction function = functionByName.get(functionName);
        if (function == null) {
            throw new IllegalArgumentException("Unknown leaf function: " + functionName);
        }
        return function;
    }
}
