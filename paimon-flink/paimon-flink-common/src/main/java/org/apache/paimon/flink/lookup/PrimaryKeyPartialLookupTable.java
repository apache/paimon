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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.query.RemoteTableQuery;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.SCAN_BOUNDED_WATERMARK;
import static org.apache.paimon.CoreOptions.STREAM_SCAN_MODE;
import static org.apache.paimon.CoreOptions.StreamScanMode.FILE_MONITOR;

/** Lookup table for primary key which supports to read the LSM tree directly. */
public class PrimaryKeyPartialLookupTable implements LookupTable {

    private final Function<Predicate, QueryExecutor> executorFactory;
    private final FixedBucketFromPkExtractor extractor;
    @Nullable private final ProjectedRow keyRearrange;
    @Nullable private final ProjectedRow trimmedKeyRearrange;

    private Predicate specificPartition;
    private QueryExecutor queryExecutor;

    private PrimaryKeyPartialLookupTable(
            Function<Predicate, QueryExecutor> executorFactory,
            FileStoreTable table,
            List<String> joinKey) {
        this.executorFactory = executorFactory;

        if (table.bucketMode() != BucketMode.HASH_FIXED) {
            throw new UnsupportedOperationException(
                    "Unsupported mode for partial lookup: " + table.bucketMode());
        }

        this.extractor = new FixedBucketFromPkExtractor(table.schema());

        ProjectedRow keyRearrange = null;
        if (!table.primaryKeys().equals(joinKey)) {
            keyRearrange =
                    ProjectedRow.from(
                            table.primaryKeys().stream()
                                    .map(joinKey::indexOf)
                                    .mapToInt(value -> value)
                                    .toArray());
        }
        this.keyRearrange = keyRearrange;

        List<String> trimmedPrimaryKeys = table.schema().trimmedPrimaryKeys();
        ProjectedRow trimmedKeyRearrange = null;
        if (!trimmedPrimaryKeys.equals(joinKey)) {
            trimmedKeyRearrange =
                    ProjectedRow.from(
                            trimmedPrimaryKeys.stream()
                                    .map(joinKey::indexOf)
                                    .mapToInt(value -> value)
                                    .toArray());
        }
        this.trimmedKeyRearrange = trimmedKeyRearrange;
    }

    @VisibleForTesting
    QueryExecutor queryExecutor() {
        return queryExecutor;
    }

    @Override
    public void specificPartitionFilter(Predicate filter) {
        this.specificPartition = filter;
    }

    @Override
    public void open() throws Exception {
        this.queryExecutor = executorFactory.apply(specificPartition);
        refresh();
    }

    @Override
    public List<InternalRow> get(InternalRow key) throws IOException {
        InternalRow adjustedKey = key;
        if (keyRearrange != null) {
            adjustedKey = keyRearrange.replaceRow(adjustedKey);
        }
        extractor.setRecord(adjustedKey);
        int bucket = extractor.bucket();
        BinaryRow partition = extractor.partition();

        InternalRow trimmedKey = key;
        if (trimmedKeyRearrange != null) {
            trimmedKey = trimmedKeyRearrange.replaceRow(trimmedKey);
        }

        InternalRow kv = queryExecutor.lookup(partition, bucket, trimmedKey);
        if (kv == null) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(kv);
        }
    }

    @Override
    public void refresh() {
        queryExecutor.refresh();
    }

    @Override
    public void close() throws IOException {
        if (queryExecutor != null) {
            queryExecutor.close();
        }
    }

    public static PrimaryKeyPartialLookupTable createLocalTable(
            FileStoreTable table,
            int[] projection,
            File tempPath,
            List<String> joinKey,
            Set<Integer> requireCachedBucketIds) {
        return new PrimaryKeyPartialLookupTable(
                filter ->
                        new LocalQueryExecutor(
                                table, projection, tempPath, filter, requireCachedBucketIds),
                table,
                joinKey);
    }

    public static PrimaryKeyPartialLookupTable createRemoteTable(
            FileStoreTable table, int[] projection, List<String> joinKey) {
        return new PrimaryKeyPartialLookupTable(
                filter -> new RemoteQueryExecutor(table, projection), table, joinKey);
    }

    interface QueryExecutor extends Closeable {

        InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException;

        void refresh();
    }

    static class LocalQueryExecutor implements QueryExecutor {

        private final LocalTableQuery tableQuery;
        private final StreamTableScan scan;

        private LocalQueryExecutor(
                FileStoreTable table,
                int[] projection,
                File tempPath,
                @Nullable Predicate filter,
                Set<Integer> requireCachedBucketIds) {
            this.tableQuery =
                    table.newLocalTableQuery()
                            .withValueProjection(Projection.of(projection).toNestedIndexes())
                            .withIOManager(new IOManagerImpl(tempPath.toString()));

            Map<String, String> dynamicOptions = new HashMap<>();
            dynamicOptions.put(STREAM_SCAN_MODE.key(), FILE_MONITOR.getValue());
            dynamicOptions.put(SCAN_BOUNDED_WATERMARK.key(), null);
            this.scan =
                    table.copy(dynamicOptions)
                            .newReadBuilder()
                            .withFilter(filter)
                            .withBucketFilter(
                                    requireCachedBucketIds == null
                                            ? null
                                            : requireCachedBucketIds::contains)
                            .newStreamScan();
        }

        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
                throws IOException {
            return tableQuery.lookup(partition, bucket, key);
        }

        @Override
        public void refresh() {
            while (true) {
                List<Split> splits = scan.plan().splits();
                if (splits.isEmpty()) {
                    return;
                }

                for (Split split : splits) {
                    if (!(split instanceof DataSplit)) {
                        throw new IllegalArgumentException(
                                "Unsupported split: " + split.getClass());
                    }
                    BinaryRow partition = ((DataSplit) split).partition();
                    int bucket = ((DataSplit) split).bucket();
                    List<DataFileMeta> before = ((DataSplit) split).beforeFiles();
                    List<DataFileMeta> after = ((DataSplit) split).dataFiles();

                    tableQuery.refreshFiles(partition, bucket, before, after);
                }
            }
        }

        @Override
        public void close() throws IOException {
            tableQuery.close();
        }
    }

    static class RemoteQueryExecutor implements QueryExecutor {

        private final RemoteTableQuery tableQuery;

        private RemoteQueryExecutor(FileStoreTable table, int[] projection) {
            this.tableQuery = new RemoteTableQuery(table).withValueProjection(projection);
        }

        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
                throws IOException {
            return tableQuery.lookup(partition, bucket, key);
        }

        @Override
        public void refresh() {}

        @Override
        public void close() throws IOException {
            tableQuery.close();
        }
    }
}
