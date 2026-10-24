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

package org.apache.paimon.table.source.splitread;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** A {@link SplitRead} for batch incremental diff. */
public class IncrementalDiffSplitRead implements SplitRead<InternalRow> {

    private static final int BEFORE_LEVEL = Integer.MIN_VALUE;
    private static final int AFTER_LEVEL = Integer.MAX_VALUE;

    private final MergeFileSplitRead mergeRead;

    private boolean forceKeepDelete = false;
    @Nullable private RowType readType;

    public IncrementalDiffSplitRead(MergeFileSplitRead mergeRead) {
        this.mergeRead = mergeRead;
    }

    @Override
    public SplitRead<InternalRow> forceKeepDelete() {
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    public SplitRead<InternalRow> withIOManager(@Nullable IOManager ioManager) {
        mergeRead.withIOManager(ioManager);
        return this;
    }

    @Override
    public SplitRead<InternalRow> withReadType(RowType readType) {
        this.readType = readType;
        // Let the merge read add its internal dependencies, such as sequence fields and the
        // source fields of event metadata, so the diff is computed on a consistent value layout.
        mergeRead.withReadType(readType);
        return this;
    }

    @Override
    public SplitRead<InternalRow> withFilter(@Nullable Predicate predicate) {
        mergeRead.withFilter(predicate);
        return this;
    }

    @Override
    public SplitRead<InternalRow> withReadBatchSizer(ReadBatchSizer sizer) {
        mergeRead.withReadBatchSizer(sizer);
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split s) throws IOException {
        IncrementalSplit split = (IncrementalSplit) s;
        // Both merge readers already project their output to the requested read type, so the
        // diff must compare and serialize values in that layout rather than the internal one.
        RowType valueType = readType != null ? readType : mergeRead.actualReadType();
        RecordReader<KeyValue> reader =
                readDiff(
                        mergeRead.createMergeReader(
                                split.partition(),
                                split.bucket(),
                                split.beforeFiles(),
                                split.beforeDeletionFiles(),
                                forceKeepDelete),
                        mergeRead.createMergeReader(
                                split.partition(),
                                split.bucket(),
                                split.afterFiles(),
                                split.afterDeletionFiles(),
                                forceKeepDelete),
                        mergeRead.keyComparator(),
                        udsComparator(valueType),
                        mergeRead.mergeSorter(),
                        InternalSerializers.create(valueType),
                        forceKeepDelete);
        return KeyValueTableRead.unwrap(reader, mergeRead.tableSchema().options());
    }

    /** The comparator only applies when every sequence field is part of the emitted values. */
    @Nullable
    private FieldsComparator udsComparator(RowType valueType) {
        List<String> fieldNames = valueType.getFieldNames();
        CoreOptions options = new CoreOptions(mergeRead.tableSchema().options());
        List<String> sequenceFields = options.sequenceField();
        if (sequenceFields.isEmpty() || !fieldNames.containsAll(sequenceFields)) {
            return null;
        }
        return UserDefinedSeqComparator.create(
                valueType, sequenceFields, options.sequenceFieldSortOrderIsAscending());
    }

    private static RecordReader<KeyValue> readDiff(
            RecordReader<KeyValue> beforeReader,
            RecordReader<KeyValue> afterReader,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeSorter sorter,
            InternalRowSerializer valueSerializer,
            boolean keepDelete)
            throws IOException {
        return sorter.mergeSortNoSpill(
                Arrays.asList(
                        () -> wrapLevelToReader(beforeReader, BEFORE_LEVEL),
                        () -> wrapLevelToReader(afterReader, AFTER_LEVEL)),
                keyComparator,
                userDefinedSeqComparator,
                new DiffMerger(keepDelete, valueSerializer));
    }

    private static RecordReader<KeyValue> wrapLevelToReader(
            RecordReader<KeyValue> reader, int level) {
        return new RecordReader<KeyValue>() {
            @Nullable
            @Override
            public RecordIterator<KeyValue> readBatch() throws IOException {
                RecordIterator<KeyValue> batch = reader.readBatch();
                if (batch == null) {
                    return null;
                }

                return new RecordIterator<KeyValue>() {
                    @Nullable
                    @Override
                    public KeyValue next() throws IOException {
                        KeyValue kv = batch.next();
                        if (kv != null) {
                            kv.setLevel(level);
                        }
                        return kv;
                    }

                    @Override
                    public void releaseBatch() {
                        batch.releaseBatch();
                    }
                };
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    private static class DiffMerger implements MergeFunctionWrapper<KeyValue> {

        private final boolean keepDelete;
        private final InternalRowSerializer serializer1;
        private final InternalRowSerializer serializer2;

        private final List<KeyValue> kvs = new ArrayList<>();

        public DiffMerger(boolean keepDelete, InternalRowSerializer serializer) {
            this.keepDelete = keepDelete;
            this.serializer1 = serializer;
            this.serializer2 = serializer.duplicate();
        }

        @Override
        public void reset() {
            this.kvs.clear();
        }

        @Override
        public void add(KeyValue kv) {
            this.kvs.add(kv);
        }

        @Nullable
        @Override
        public KeyValue getResult() {
            KeyValue toReturn = null;
            if (kvs.size() == 1) {
                KeyValue kv = kvs.get(0);
                if (kv.level() == BEFORE_LEVEL) {
                    if (keepDelete && kv.isAdd()) {
                        return kv.replaceValueKind(RowKind.DELETE);
                    }
                } else {
                    toReturn = kv;
                }
            } else if (kvs.size() == 2) {
                KeyValue before = kvs.get(0);
                KeyValue after = kvs.get(1);
                if (!valueAndRowKindEquals(before, after)) {
                    toReturn = after.level() == AFTER_LEVEL ? after : before;
                }
            } else {
                throw new IllegalArgumentException("Illegal kv number: " + kvs.size());
            }

            if (toReturn != null && (keepDelete || toReturn.isAdd())) {
                return toReturn;
            }

            return null;
        }

        private boolean valueAndRowKindEquals(KeyValue before, KeyValue after) {
            return serializer1
                            .toBinaryRow(before.value())
                            .equals(serializer2.toBinaryRow(after.value()))
                    && before.isAdd() == after.isAdd();
        }
    }
}
