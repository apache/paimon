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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.types.RowType;

import java.util.Comparator;

/** Compacted row keys, compared by their typed components with nulls first. */
public class CompositeKeySerializer implements KeySerializer {

    private final ThreadLocal<RowCompactedSerializer> serializer;
    private final InternalRow.FieldGetter[] getters;
    private final Comparator<Object>[] comparators;

    @SuppressWarnings("unchecked")
    public CompositeKeySerializer(RowType type) {
        serializer = ThreadLocal.withInitial(() -> new RowCompactedSerializer(type));
        int count = type.getFieldCount();
        getters = new InternalRow.FieldGetter[count];
        comparators = new Comparator[count];
        for (int i = 0; i < count; i++) {
            getters[i] = InternalRow.createFieldGetter(type.getTypeAt(i), i);
            comparators[i] = KeySerializer.create(type.getTypeAt(i)).createComparator();
        }
    }

    @Override
    public byte[] serialize(Object key) {
        // Canonicalize RowKind and NaN payloads so equal keys have the same Bloom hash.
        GenericRow row = new GenericRow(getters.length);
        for (int i = 0; i < getters.length; i++) {
            Object value = getters[i].getFieldOrNull((InternalRow) key);
            if (value instanceof Float && Float.isNaN((Float) value)) {
                value = Float.NaN;
            } else if (value instanceof Double && Double.isNaN((Double) value)) {
                value = Double.NaN;
            }
            row.setField(i, value);
        }
        return serializer.get().serializeToBytes(row);
    }

    @Override
    public Object deserialize(MemorySlice data) {
        return serializer.get().deserialize(data.copyBytes());
    }

    @Override
    public Comparator<MemorySlice> createSliceComparator() {
        return serializer.get().createSliceComparator();
    }

    @Override
    public Comparator<Object> createComparator() {
        return (left, right) -> {
            for (int i = 0; i < getters.length; i++) {
                Object a = getters[i].getFieldOrNull((InternalRow) left);
                Object b = getters[i].getFieldOrNull((InternalRow) right);
                int comparison =
                        a == null
                                ? (b == null ? 0 : -1)
                                : b == null ? 1 : comparators[i].compare(a, b);
                if (comparison != 0) {
                    return comparison;
                }
            }
            return 0;
        };
    }
}
