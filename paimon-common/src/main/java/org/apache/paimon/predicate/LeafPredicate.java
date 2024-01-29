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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.utils.InternalRowUtils.get;

/** Leaf node of a {@link Predicate} tree. Compares a field in the row with literals. */
public class LeafPredicate implements Predicate {

    private static final long serialVersionUID = 1L;

    private final LeafFunction function;
    private final DataType type;
    private final int fieldIndex;
    private final String fieldName;

    private transient List<Object> literals;

    public LeafPredicate(
            LeafFunction function,
            DataType type,
            int fieldIndex,
            String fieldName,
            List<Object> literals) {
        this.function = function;
        this.type = type;
        this.fieldIndex = fieldIndex;
        this.fieldName = fieldName;
        this.literals = literals;
    }

    public LeafFunction function() {
        return function;
    }

    public DataType type() {
        return type;
    }

    public int index() {
        return fieldIndex;
    }

    public String fieldName() {
        return fieldName;
    }

    public FieldRef fieldRef() {
        return new FieldRef(fieldIndex, fieldName, type);
    }

    public List<Object> literals() {
        return literals;
    }

    public LeafPredicate copyWithNewIndex(int fieldIndex) {
        return new LeafPredicate(function, type, fieldIndex, fieldName, literals);
    }

    @Override
    public boolean test(InternalRow row) {
        return function.test(type, get(row, fieldIndex, type), literals);
    }

    @Override
    public boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts) {
        Object min = get(minValues, fieldIndex, type);
        Object max = get(maxValues, fieldIndex, type);
        Long nullCount = nullCounts.isNullAt(fieldIndex) ? null : nullCounts.getLong(fieldIndex);
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
        return function.negate()
                .map(negate -> new LeafPredicate(negate, type, fieldIndex, fieldName, literals));
    }

    @Override
    public <T> T visit(PredicateVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LeafPredicate that = (LeafPredicate) o;
        return fieldIndex == that.fieldIndex
                && Objects.equals(fieldName, that.fieldName)
                && Objects.equals(function, that.function)
                && Objects.equals(type, that.type)
                && Objects.equals(literals, that.literals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, type, fieldIndex, fieldName, literals);
    }

    @Override
    public String toString() {
        String literalsStr;
        if (literals == null || literals.isEmpty()) {
            literalsStr = "";
        } else if (literals.size() == 1) {
            literalsStr = Objects.toString(literals.get(0));
        } else {
            literalsStr = literals.toString();
        }
        return literalsStr.isEmpty()
                ? function + "(" + fieldName + ")"
                : function + "(" + fieldName + ", " + literalsStr + ")";
    }

    private ListSerializer<Object> objectsSerializer() {
        return new ListSerializer<>(
                NullableSerializer.wrapIfNullIsNotSupported(InternalSerializers.create(type)));
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        objectsSerializer().serialize(literals, new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        literals = objectsSerializer().deserialize(new DataInputViewStreamWrapper(in));
    }
}
