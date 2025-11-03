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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.utils.InternalRowUtils.get;

/** Leaf node of a {@link Predicate} tree. Compares a field in the row with literals. */
public class LeafPredicate extends TransformPredicate {

    private static final long serialVersionUID = 2L;

    public LeafPredicate(
            LeafFunction function,
            DataType type,
            int fieldIndex,
            String fieldName,
            List<Object> literals) {
        this(new FieldTransform(new FieldRef(fieldIndex, fieldName, type)), function, literals);
    }

    public LeafPredicate(
            FieldTransform fieldTransform, LeafFunction function, List<Object> literals) {
        super(fieldTransform, function, literals);
    }

    public LeafFunction function() {
        return function;
    }

    public DataType type() {
        return fieldRef().type();
    }

    public int index() {
        return fieldRef().index();
    }

    public String fieldName() {
        return fieldRef().name();
    }

    public List<String> fieldNames() {
        return Collections.singletonList(fieldRef().name());
    }

    public FieldRef fieldRef() {
        return ((FieldTransform) transform).fieldRef();
    }

    public List<Object> literals() {
        return literals;
    }

    public LeafPredicate copyWithNewIndex(int fieldIndex) {
        return new LeafPredicate(function, type(), fieldIndex, fieldName(), literals);
    }

    @Override
    public boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts) {
        Object min = get(minValues, index(), type());
        Object max = get(maxValues, index(), type());
        Long nullCount = nullCounts.isNullAt(index()) ? null : nullCounts.getLong(index());
        if (nullCount == null || rowCount != nullCount) {
            // not all null
            // min or max is null
            // unknown stats
            if (min == null || max == null) {
                return true;
            }
        }
        return function.test(type(), rowCount, min, max, nullCount, literals);
    }

    @Override
    public Optional<Predicate> negate() {
        return function.negate()
                .map(negate -> new LeafPredicate(negate, type(), index(), fieldName(), literals));
    }

    @Override
    public <T> T visit(PredicateVisitor<T> visitor) {
        return visitor.visit(this);
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
                ? function + "(" + fieldName() + ")"
                : function + "(" + fieldName() + ", " + literalsStr + ")";
    }

    private ListSerializer<Object> objectsSerializer() {
        return new ListSerializer<>(
                NullableSerializer.wrapIfNullIsNotSupported(InternalSerializers.create(type())));
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
