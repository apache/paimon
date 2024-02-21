/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBState;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.PartialRow;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.schema.SystemColumns.SEQUENCE_NUMBER;

/** Lookup table of full cache. */
public abstract class FullCacheLookupTable implements LookupTable {

    protected final Context context;
    protected final RocksDBStateFactory stateFactory;
    protected final RowType projectedType;
    private final TableStreamingReader reader;
    private final boolean sequenceFieldEnabled;

    public FullCacheLookupTable(Context context) throws IOException {
        this.context = context;
        this.stateFactory =
                new RocksDBStateFactory(
                        context.tempPath.toString(),
                        context.table.coreOptions().toConfiguration(),
                        null);
        FileStoreTable table = context.table;
        this.reader = new TableStreamingReader(table, context.projection, context.tablePredicate);
        this.sequenceFieldEnabled =
                table.primaryKeys().size() > 0
                        && new CoreOptions(table.options()).sequenceField().isPresent();
        RowType projectedType = TypeUtils.project(table.rowType(), context.projection);
        if (sequenceFieldEnabled) {
            projectedType = projectedType.appendDataField(SEQUENCE_NUMBER, DataTypes.BIGINT());
        }
        this.projectedType = projectedType;
    }

    @Override
    public void open() throws Exception {
        BinaryExternalSortBuffer bulkLoadSorter =
                RocksDBState.createBulkLoadSorter(
                        IOManager.create(context.tempPath.toString()), context.table.coreOptions());
        Predicate predicate = projectedPredicate();
        try (RecordReaderIterator<InternalRow> batch =
                new RecordReaderIterator<>(reader.nextBatch(true, sequenceFieldEnabled))) {
            while (batch.hasNext()) {
                InternalRow row = batch.next();
                if (predicate == null || predicate.test(row)) {
                    bulkLoadSorter.write(GenericRow.of(toKeyBytes(row), toValueBytes(row)));
                }
            }
        }

        MutableObjectIterator<BinaryRow> keyIterator = bulkLoadSorter.sortedIterator();
        BinaryRow row = new BinaryRow(2);
        TableBulkLoader bulkLoader = createBulkLoader();
        try {
            while ((row = keyIterator.next(row)) != null) {
                bulkLoader.write(row.getBinary(0), row.getBinary(1));
            }
        } catch (BulkLoader.WriteException e) {
            throw new RuntimeException(
                    "Exception in bulkLoad, the most suspicious reason is that "
                            + "your data contains duplicates, please check your lookup table. ",
                    e.getCause());
        }

        bulkLoader.finish();
        bulkLoadSorter.clear();
    }

    @Override
    public void refresh() throws Exception {
        while (true) {
            try (RecordReaderIterator<InternalRow> batch =
                    new RecordReaderIterator<>(reader.nextBatch(false, sequenceFieldEnabled))) {
                if (!batch.hasNext()) {
                    return;
                }
                refresh(batch, sequenceFieldEnabled);
            }
        }
    }

    @Override
    public final List<InternalRow> get(InternalRow key) throws IOException {
        List<InternalRow> values = innerGet(key);
        if (!sequenceFieldEnabled) {
            return values;
        }

        List<InternalRow> dropSequence = new ArrayList<>(values.size());
        for (InternalRow matchedRow : values) {
            dropSequence.add(new PartialRow(matchedRow.getFieldCount() - 1, matchedRow));
        }
        return dropSequence;
    }

    public abstract List<InternalRow> innerGet(InternalRow key) throws IOException;

    public abstract void refresh(Iterator<InternalRow> input, boolean orderByLastField)
            throws IOException;

    @Nullable
    public Predicate projectedPredicate() {
        return context.projectedPredicate;
    }

    public abstract byte[] toKeyBytes(InternalRow row) throws IOException;

    public abstract byte[] toValueBytes(InternalRow row) throws IOException;

    public abstract TableBulkLoader createBulkLoader();

    @Override
    public void close() throws IOException {
        stateFactory.close();
    }

    /** Bulk loader for the table. */
    public interface TableBulkLoader {

        void write(byte[] key, byte[] value) throws BulkLoader.WriteException, IOException;

        void finish() throws IOException;
    }

    static FullCacheLookupTable create(Context context, long lruCacheSize) throws IOException {
        List<String> primaryKeys = context.table.primaryKeys();
        if (primaryKeys.isEmpty()) {
            return new NoPrimaryKeyLookupTable(context, lruCacheSize);
        } else {
            if (new HashSet<>(primaryKeys).equals(new HashSet<>(context.joinKey))) {
                return new PrimaryKeyLookupTable(context, lruCacheSize, context.joinKey);
            } else {
                return new SecondaryIndexLookupTable(context, lruCacheSize);
            }
        }
    }

    /** Context for {@link LookupTable}. */
    public static class Context {

        public final FileStoreTable table;
        public final int[] projection;
        @Nullable public final Predicate tablePredicate;
        @Nullable public final Predicate projectedPredicate;
        public final File tempPath;
        public final List<String> joinKey;

        public Context(
                FileStoreTable table,
                int[] projection,
                @Nullable Predicate tablePredicate,
                @Nullable Predicate projectedPredicate,
                File tempPath,
                List<String> joinKey) {
            this.table = table;
            this.projection = projection;
            this.tablePredicate = tablePredicate;
            this.projectedPredicate = projectedPredicate;
            this.tempPath = tempPath;
            this.joinKey = joinKey;
        }
    }
}
