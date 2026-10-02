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
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.types.RowType;

import java.util.Comparator;

/** Length-delimited tuple keys, compared by their typed components with nulls first. */
public class CompositeKeySerializer implements KeySerializer {

    private final RowType rowType;

    private final KeySerializer[] serializers;
    private final InternalRow.FieldGetter[] getters;
    private final Comparator<Object>[] comparators;

    @SuppressWarnings("unchecked")
    public CompositeKeySerializer(RowType type) {
        this.rowType = type;
        int count = type.getFieldCount();
        serializers = new KeySerializer[count];
        getters = new InternalRow.FieldGetter[count];
        comparators = new Comparator[count];
        for (int i = 0; i < count; i++) {
            serializers[i] = KeySerializer.create(type.getTypeAt(i));
            getters[i] = InternalRow.createFieldGetter(type.getTypeAt(i), i);
            comparators[i] = serializers[i].createComparator();
        }
    }

    public RowType rowType() {
        return rowType;
    }

    @Override
    public byte[] serialize(Object key) {
        InternalRow row = (InternalRow) key;
        if (row.getFieldCount() != serializers.length) {
            throw new IllegalArgumentException(
                    "Expected "
                            + serializers.length
                            + " composite key fields, but got "
                            + row.getFieldCount());
        }
        MemorySliceOutput output = new MemorySliceOutput(32);
        for (int i = 0; i < serializers.length; i++) {
            Object value = getters[i].getFieldOrNull(row);
            if (value == null) {
                output.writeInt(-1);
            } else {
                byte[] bytes = serializers[i].serialize(value);
                output.writeInt(bytes.length);
                output.writeBytes(bytes);
            }
        }
        return output.toSlice().copyBytes();
    }

    @Override
    public Object deserialize(MemorySlice data) {
        MemorySliceInput input = data.toInput();
        GenericRow row = new GenericRow(serializers.length);
        for (int i = 0; i < serializers.length; i++) {
            int length = input.readInt();
            if (length >= 0) {
                row.setField(i, serializers[i].deserialize(input.readSlice(length)));
            } else if (length != -1) {
                throw new IllegalArgumentException(
                        "Invalid composite key component length: " + length);
            }
        }
        if (input.available() != 0) {
            throw new IllegalArgumentException("Trailing bytes in composite key");
        }
        return row;
    }

    @Override
    public Comparator<Object> createComparator() {
        return (left, right) -> {
            for (int i = 0; i < serializers.length; i++) {
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
