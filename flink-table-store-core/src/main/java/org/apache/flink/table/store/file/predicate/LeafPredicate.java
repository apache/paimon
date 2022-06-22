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

package org.apache.flink.table.store.file.predicate;

import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Leaf node of a {@link Predicate} tree. Compares a field in the row with literals. */
public class LeafPredicate implements Predicate {

    private static final long serialVersionUID = 1L;

    private final LeafFunction function;
    private final LogicalType type;
    private final int index;

    private transient List<Object> literals;

    public LeafPredicate(
            LeafFunction function, LogicalType type, int index, List<Object> literals) {
        this.function = function;
        this.type = type;
        this.index = index;
        this.literals = literals;
    }

    public LeafFunction function() {
        return function;
    }

    @Nullable
    public LogicalType type() {
        return type;
    }

    public int index() {
        return index;
    }

    public List<Object> literals() {
        return literals;
    }

    @Override
    public boolean test(Object[] values) {
        return function.test(type, values[index], literals);
    }

    @Override
    public boolean test(long rowCount, FieldStats[] fieldStats) {
        return function.test(type, rowCount, fieldStats[index], literals);
    }

    @Override
    public Optional<Predicate> negate() {
        return function.negate().map(negate -> new LeafPredicate(negate, type, index, literals));
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
        return index == that.index
                && Objects.equals(function, that.function)
                && Objects.equals(type, that.type)
                && Objects.equals(literals, that.literals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, type, index, literals);
    }

    private ListSerializer<Object> objectsSerializer() {
        return new ListSerializer<>(
                NullableSerializer.wrapIfNullIsNotSupported(
                        InternalSerializers.create(type), false));
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
