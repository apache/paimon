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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.ListSerializer;
import org.apache.paimon.data.serializer.NullableSerializer;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** A {@link Predicate} with {@link Transform}. */
public class TransformPredicate implements Predicate {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_TRANSFORM = "transform";

    public static final String FIELD_FUNCTION = "function";

    public static final String FIELD_LITERALS = "literals";

    @JsonProperty(FIELD_TRANSFORM)
    protected final Transform transform;

    @JsonProperty(FIELD_FUNCTION)
    protected final LeafFunction function;

    protected transient List<Object> literals;

    protected TransformPredicate(
            Transform transform, LeafFunction function, List<Object> literals) {
        this.transform = transform;
        this.function = function;
        this.literals = literals;
    }

    @JsonCreator
    protected static TransformPredicate fromJson(
            @JsonProperty(TransformPredicate.FIELD_TRANSFORM) Transform transform,
            @JsonProperty(TransformPredicate.FIELD_FUNCTION) LeafFunction function,
            @JsonProperty(TransformPredicate.FIELD_LITERALS) List<Object> literals) {
        List<Object> convertedLiterals = deserializeLiterals(transform.outputType(), literals);
        if (transform instanceof FieldTransform) {
            return new LeafPredicate((FieldTransform) transform, function, convertedLiterals);
        }
        return new TransformPredicate(transform, function, convertedLiterals);
    }

    public static TransformPredicate of(
            Transform transform, LeafFunction function, List<Object> literals) {
        if (transform instanceof FieldTransform) {
            return new LeafPredicate((FieldTransform) transform, function, literals);
        }
        return new TransformPredicate(transform, function, literals);
    }

    @JsonGetter(FIELD_TRANSFORM)
    public Transform transform() {
        return transform;
    }

    @JsonGetter(FIELD_LITERALS)
    public List<Object> literalsForJson() {
        return serializeLiterals(transform.outputType(), literals);
    }

    public TransformPredicate copyWithNewInputs(List<Object> newInputs) {
        return TransformPredicate.of(transform.copyWithNewInputs(newInputs), function, literals);
    }

    public List<String> fieldNames() {
        List<String> names = new ArrayList<>();
        for (Object input : transform.inputs()) {
            if (input instanceof FieldRef) {
                names.add(((FieldRef) input).name());
            }
        }
        return names;
    }

    @Override
    public boolean test(InternalRow row) {
        Object value = transform.transform(row);
        return function.test(transform.outputType(), value, literals);
    }

    @Override
    public boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts) {
        return true;
    }

    @Override
    public Optional<Predicate> negate() {
        return Optional.empty();
    }

    @Override
    public <T> T visit(PredicateVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransformPredicate that = (TransformPredicate) o;
        return Objects.equals(transform, that.transform)
                && Objects.equals(function, that.function)
                && Objects.equals(literals, that.literals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transform, function, literals);
    }

    @Override
    public String toString() {
        return "TransformPredicate{"
                + "transform="
                + transform
                + ", function="
                + function
                + ", literals="
                + literals
                + '}';
    }

    private ListSerializer<Object> objectsSerializer() {
        return new ListSerializer<>(
                NullableSerializer.wrapIfNullIsNotSupported(
                        InternalSerializers.create(transform.outputType())));
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        objectsSerializer().serialize(literals, new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        literals = objectsSerializer().deserialize(new DataInputViewStreamWrapper(in));
    }

    protected static List<Object> serializeLiterals(DataType type, List<Object> literals) {
        if (literals == null) {
            return null;
        }
        List<Object> serialized = new ArrayList<>(literals.size());
        for (Object lit : literals) {
            serialized.add(PredicateBuilder.convertToJavaObject(type, lit));
        }
        return serialized;
    }

    protected static List<Object> deserializeLiterals(DataType type, List<Object> literals) {
        if (literals == null) {
            return null;
        }
        List<Object> converted = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            if (literal instanceof DataType) {
                converted.add(literal);
                continue;
            }
            converted.add(PredicateBuilder.convertJavaObject(type, literal));
        }
        return converted;
    }
}
