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
import org.apache.paimon.utils.StringUtils;

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

import static org.apache.paimon.utils.InternalRowUtils.get;

/** Leaf node of a {@link Predicate} tree. Compares a field in the row with literals. */
public class LeafPredicate implements Predicate {

    private static final long serialVersionUID = 3L;

    public static final String FIELD_TRANSFORM = "transform";

    public static final String FIELD_FUNCTION = "function";

    public static final String FIELD_LITERALS = "literals";

    @JsonProperty(FIELD_TRANSFORM)
    private final Transform transform;

    @JsonProperty(FIELD_FUNCTION)
    private final LeafFunction function;

    private transient List<Object> literals;

    public LeafPredicate(
            LeafFunction function,
            DataType type,
            int fieldIndex,
            String fieldName,
            List<Object> literals) {
        this(new FieldTransform(new FieldRef(fieldIndex, fieldName, type)), function, literals);
    }

    public LeafPredicate(Transform transform, LeafFunction function, List<Object> literals) {
        this.transform = transform;
        this.function = function;
        this.literals = literals;
    }

    public static LeafPredicate of(
            Transform transform, LeafFunction function, List<Object> literals) {
        return new LeafPredicate(transform, function, literals);
    }

    @JsonCreator
    protected static LeafPredicate fromJson(
            @JsonProperty(FIELD_TRANSFORM) Transform transform,
            @JsonProperty(FIELD_FUNCTION) LeafFunction function,
            @JsonProperty(FIELD_LITERALS) List<Object> literals) {
        List<Object> convertedLiterals = deserializeLiterals(transform.outputType(), literals);
        return new LeafPredicate(transform, function, convertedLiterals);
    }

    public LeafPredicate copyWithNewInputs(List<Object> newInputs) {
        return new LeafPredicate(transform.copyWithNewInputs(newInputs), function, literals);
    }

    @JsonGetter(FIELD_TRANSFORM)
    public Transform transform() {
        return transform;
    }

    @JsonGetter(FIELD_FUNCTION)
    public LeafFunction function() {
        return function;
    }

    @JsonGetter(FIELD_LITERALS)
    public List<Object> literalsForJson() {
        return serializeLiterals(transform.outputType(), literals);
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

    public Optional<FieldRef> fieldRefOptional() {
        if (transform instanceof FieldTransform) {
            return Optional.of(((FieldTransform) transform).fieldRef());
        }
        return Optional.empty();
    }

    public List<Object> literals() {
        return literals;
    }

    @Override
    public boolean test(InternalRow row) {
        Object value = transform.transform(row);
        return function.test(transform.outputType(), value, literals);
    }

    @Override
    public boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts) {
        Optional<FieldRef> fieldRefOptional = fieldRefOptional();
        if (!fieldRefOptional.isPresent()) {
            return true;
        }
        FieldRef fieldRef = fieldRefOptional.get();
        int index = fieldRef.index();
        DataType type = fieldRef.type();

        Object min = get(minValues, index, type);
        Object max = get(maxValues, index, type);
        Long nullCount = nullCounts.isNullAt(index) ? null : nullCounts.getLong(index);
        if (nullCount == null || rowCount != nullCount) {
            // not all null
            // min or max is null
            // unknown stats
            if (min == null || max == null) {
                return true;
            }
        }
        return function.test(type, rowCount, min, max, nullCount, literals);
    }

    @Override
    public Optional<Predicate> negate() {
        Optional<FieldRef> fieldRefOptional = fieldRefOptional();
        if (!fieldRefOptional.isPresent()) {
            return Optional.empty();
        }
        FieldRef fieldRef = fieldRefOptional.get();
        return function.negate()
                .map(
                        negate ->
                                new LeafPredicate(
                                        negate,
                                        fieldRef.type(),
                                        fieldRef.index(),
                                        fieldRef.name(),
                                        literals));
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
        LeafPredicate that = (LeafPredicate) o;
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
        String literalsStr;
        int literalsSize = literals == null ? 0 : literals.size();
        if (literalsSize == 0) {
            literalsStr = "";
        } else if (literalsSize == 1) {
            literalsStr = Objects.toString(literals.get(0));
        } else {
            literalsStr = StringUtils.truncatedString(literals, "[", ", ", "]");
        }
        return literalsStr.isEmpty()
                ? function + "(" + transform + ")"
                : function + "(" + transform + ", " + literalsStr + ")";
    }

    private ListSerializer<Object> literalsSerializer() {
        return new ListSerializer<>(
                NullableSerializer.wrapIfNullIsNotSupported(
                        InternalSerializers.create(transform.outputType())));
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        literalsSerializer().serialize(literals, new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        literals = literalsSerializer().deserialize(new DataInputViewStreamWrapper(in));
    }

    // ====================== Deprecated methods ===============================
    // ================ Will be removed in Next Version ========================

    /** Use {@link #fieldRefOptional()} instead. */
    @Deprecated
    public DataType type() {
        return fieldRef().type();
    }

    /** Use {@link #fieldRefOptional()} instead. */
    @Deprecated
    public int index() {
        return fieldRef().index();
    }

    /** Use {@link #fieldRefOptional()} instead. */
    @Deprecated
    public String fieldName() {
        return fieldRef().name();
    }

    /** Use {@link #fieldRefOptional()} instead. */
    @Deprecated
    public FieldRef fieldRef() {
        //noinspection OptionalGetWithoutIsPresent
        return fieldRefOptional().get();
    }

    /** Use {@link #fieldRefOptional()} instead. */
    @Deprecated
    public LeafPredicate copyWithNewIndex(int fieldIndex) {
        return new LeafPredicate(function, type(), fieldIndex, fieldName(), literals);
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
