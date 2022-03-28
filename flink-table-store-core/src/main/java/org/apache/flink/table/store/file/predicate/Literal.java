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

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

/** A serializable literal class. */
public class Literal implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final BytePrimitiveArrayComparator BINARY_COMPARATOR =
            new BytePrimitiveArrayComparator(true);

    private final LogicalType type;

    private transient Object value;

    public Literal(LogicalType type, Object value) {
        this.type = type;
        this.value = value;
    }

    public LogicalType type() {
        return type;
    }

    public Object value() {
        return value;
    }

    public int compareValueTo(Object o) {
        if (value instanceof Comparable) {
            return ((Comparable<Object>) value).compareTo(o);
        } else if (value instanceof byte[]) {
            return BINARY_COMPARATOR.compare((byte[]) value, (byte[]) o);
        } else {
            throw new RuntimeException("Unsupported type: " + type);
        }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        InternalSerializers.create(type).serialize(value, new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        value = InternalSerializers.create(type).deserialize(new DataInputViewStreamWrapper(in));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Literal)) {
            return false;
        }
        Literal literal = (Literal) o;
        return type.equals(literal.type) && value.equals(literal.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }
}
