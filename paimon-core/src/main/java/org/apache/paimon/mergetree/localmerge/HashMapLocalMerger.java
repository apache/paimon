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

package org.apache.paimon.mergetree.localmerge;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldSetter;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.hash.BytesHashMap;
import org.apache.paimon.hash.BytesMap.LookupInfo;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.KeyValueIterator;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.paimon.data.InternalRow.createFieldSetter;

/** A {@link LocalMerger} which stores records in {@link BytesHashMap}. */
public class HashMapLocalMerger implements LocalMerger {

    private final InternalRowSerializer valueSerializer;
    private final MergeFunction<KeyValue> mergeFunction;
    @Nullable private final FieldsComparator udsComparator;
    private final BytesHashMap<BinaryRow> buffer;
    private final List<FieldSetter> nonKeySetters;

    public HashMapLocalMerger(
            RowType rowType,
            List<String> primaryKeys,
            MemorySegmentPool memoryPool,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable FieldsComparator userDefinedSeqComparator) {
        this.valueSerializer = new InternalRowSerializer(rowType);
        this.mergeFunction = mergeFunction;
        this.udsComparator = userDefinedSeqComparator;
        this.buffer =
                new BytesHashMap<>(
                        memoryPool,
                        new BinaryRowSerializer(primaryKeys.size()),
                        rowType.getFieldCount());

        this.nonKeySetters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataField field = rowType.getFields().get(i);
            if (primaryKeys.contains(field.name())) {
                continue;
            }
            nonKeySetters.add(createFieldSetter(field.type(), i));
        }
    }

    @Override
    public boolean put(RowKind rowKind, BinaryRow key, InternalRow value) throws IOException {
        // we store row kind in value
        value.setRowKind(rowKind);

        LookupInfo<BinaryRow, BinaryRow> lookup = buffer.lookup(key);
        if (!lookup.isFound()) {
            try {
                buffer.append(lookup, valueSerializer.toBinaryRow(value));
                return true;
            } catch (EOFException eof) {
                return false;
            }
        }

        mergeFunction.reset();
        BinaryRow stored = lookup.getValue();
        KeyValue previousKv = new KeyValue().replace(key, stored.getRowKind(), stored);
        KeyValue newKv = new KeyValue().replace(key, value.getRowKind(), value);
        if (udsComparator != null && udsComparator.compare(stored, value) > 0) {
            mergeFunction.add(newKv);
            mergeFunction.add(previousKv);
        } else {
            mergeFunction.add(previousKv);
            mergeFunction.add(newKv);
        }

        KeyValue result = mergeFunction.getResult();
        stored.setRowKind(result.valueKind());
        for (FieldSetter setter : nonKeySetters) {
            setter.setFieldFrom(result.value(), stored);
        }
        return true;
    }

    @Override
    public int size() {
        return buffer.getNumElements();
    }

    @Override
    public void forEach(Consumer<InternalRow> consumer) throws IOException {
        KeyValueIterator<BinaryRow, BinaryRow> iterator = buffer.getEntryIterator(false);
        while (iterator.advanceNext()) {
            consumer.accept(iterator.getValue());
        }
    }

    @Override
    public void clear() {
        buffer.reset();
    }
}
