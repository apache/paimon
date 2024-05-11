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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBState;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.PartialRow;
import org.apache.paimon.utils.TypeUtils;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_ASYNC;

/** Lookup table of full cache. */
public abstract class FullCacheLookupTable implements LookupTable {
    private static final Logger LOG = LoggerFactory.getLogger(FullCacheLookupTable.class);

    protected final Object lock = new Object();
    protected final Context context;
    protected final RowType projectedType;
    protected final boolean refreshAsync;

    @Nullable protected final FieldsComparator userDefinedSeqComparator;
    protected final int appendUdsFieldNumber;

    protected RocksDBStateFactory stateFactory;
    private final ExecutorService refreshExecutor;
    private final AtomicReference<Exception> cachedException;
    private LookupStreamingReader reader;
    private Predicate specificPartition;

    public FullCacheLookupTable(Context context) {
        this.context = context;
        FileStoreTable table = context.table;
        List<String> sequenceFields = new ArrayList<>();
        if (table.primaryKeys().size() > 0) {
            sequenceFields = new CoreOptions(table.options()).sequenceField();
        }
        RowType projectedType = TypeUtils.project(table.rowType(), context.projection);
        if (sequenceFields.size() > 0) {
            RowType.Builder builder = RowType.builder();
            projectedType.getFields().forEach(f -> builder.field(f.name(), f.type()));
            RowType rowType = table.rowType();
            AtomicInteger appendUdsFieldNumber = new AtomicInteger(0);
            sequenceFields.stream()
                    .filter(projectedType::notContainsField)
                    .map(rowType::getField)
                    .forEach(
                            f -> {
                                appendUdsFieldNumber.incrementAndGet();
                                builder.field(f.name(), f.type());
                            });
            projectedType = builder.build();
            this.userDefinedSeqComparator =
                    UserDefinedSeqComparator.create(projectedType, sequenceFields);
            this.appendUdsFieldNumber = appendUdsFieldNumber.get();
        } else {
            this.userDefinedSeqComparator = null;
            this.appendUdsFieldNumber = 0;
        }
        this.projectedType = projectedType;
        this.refreshAsync = Options.fromMap(context.table.options()).get(LOOKUP_REFRESH_ASYNC);
        this.refreshExecutor = this.refreshAsync ? Executors.newSingleThreadExecutor() : null;
        this.cachedException = new AtomicReference<>();
    }

    @Override
    public void specificPartitionFilter(Predicate filter) {
        this.specificPartition = filter;
    }

    protected void openStateFactory() throws Exception {
        this.stateFactory =
                new RocksDBStateFactory(
                        context.tempPath.toString(),
                        context.table.coreOptions().toConfiguration(),
                        null);
    }

    protected void bootstrap() throws Exception {
        Predicate scanPredicate =
                PredicateBuilder.andNullable(context.tablePredicate, specificPartition);
        this.reader = new LookupStreamingReader(context.table, context.projection, scanPredicate);
        BinaryExternalSortBuffer bulkLoadSorter =
                RocksDBState.createBulkLoadSorter(
                        IOManager.create(context.tempPath.toString()), context.table.coreOptions());
        Predicate predicate = projectedPredicate();
        try (RecordReaderIterator<InternalRow> batch =
                new RecordReaderIterator<>(reader.nextBatch(true))) {
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
        if (refreshAsync) {
            try {
                refreshExecutor.execute(
                        () -> {
                            try {
                                doRefresh();
                            } catch (Exception e) {
                                LOG.error(
                                        "Refresh lookup table {} failed", context.table.name(), e);
                                cachedException.set(e);
                            }
                        });
            } catch (RejectedExecutionException e) {
                LOG.warn("Add refresh task for lookup table {} failed", context.table.name(), e);
            }
        } else {
            doRefresh();
        }
    }

    private void doRefresh() throws Exception {
        while (true) {
            try (RecordReaderIterator<InternalRow> batch =
                    new RecordReaderIterator<>(reader.nextBatch(false))) {
                if (!batch.hasNext()) {
                    return;
                }
                refresh(batch);
            }
        }
    }

    @Override
    public final List<InternalRow> get(InternalRow key) throws IOException {
        List<InternalRow> values;
        if (refreshAsync) {
            synchronized (lock) {
                values = innerGet(key);
            }
        } else {
            values = innerGet(key);
        }
        if (appendUdsFieldNumber == 0) {
            return values;
        }

        List<InternalRow> dropSequence = new ArrayList<>(values.size());
        for (InternalRow matchedRow : values) {
            dropSequence.add(
                    new PartialRow(matchedRow.getFieldCount() - appendUdsFieldNumber, matchedRow));
        }
        return dropSequence;
    }

    public void refresh(Iterator<InternalRow> input) throws IOException {
        Predicate predicate = projectedPredicate();
        while (input.hasNext()) {
            InternalRow row = input.next();
            if (refreshAsync) {
                synchronized (lock) {
                    refreshRow(row, predicate);
                }
            } else {
                refreshRow(row, predicate);
            }
        }
    }

    public abstract List<InternalRow> innerGet(InternalRow key) throws IOException;

    protected abstract void refreshRow(InternalRow row, Predicate predicate) throws IOException;

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
        if (refreshExecutor != null) {
            refreshExecutor.shutdown();
        }
        FileIOUtils.deleteDirectory(context.tempPath);
    }

    /** Bulk loader for the table. */
    public interface TableBulkLoader {

        void write(byte[] key, byte[] value) throws BulkLoader.WriteException, IOException;

        void finish() throws IOException;
    }

    static FullCacheLookupTable create(Context context, long lruCacheSize) {
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
