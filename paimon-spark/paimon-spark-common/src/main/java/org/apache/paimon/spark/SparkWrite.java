/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.PartitionIndex;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.write.V1Write;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.sources.InsertableRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Spark {@link V1Write}, it is required to use v1 write for grouping by bucket. */
public class SparkWrite implements V1Write {

    private final Table table;
    private final Lock.Factory lockFactory;

    private final CommitMessageSerializer serializer = new CommitMessageSerializer();

    public SparkWrite(Table table, Lock.Factory lockFactory) {
        this.table = table;
        this.lockFactory = lockFactory;
    }

    @Override
    public InsertableRelation toInsertableRelation() {
        return (data, overwrite) -> {
            if (overwrite) {
                throw new UnsupportedOperationException("Overwrite is unsupported.");
            }

            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            JavaPairRDD<Integer, Iterable<InternalRow>> grouped;
            if (isDynamicBucketTable()) {
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                Dataset<Row> partitioned = data;
                if (!table.partitionKeys().isEmpty()) {
                    // repartition data in order to reduce the number of PartitionIndex for per
                    // Spark Task.
                    partitioned =
                            data.repartition(
                                    table.partitionKeys().stream()
                                            .map(functions::col)
                                            .toArray(Column[]::new));
                }
                grouped =
                        partitioned
                                .toJavaRDD()
                                .mapPartitions(
                                        rows -> assignBucket(fileStoreTable, writeBuilder, rows))
                                .groupBy(row -> ((DynamicBucketRow) row).bucket());
            } else {
                grouped =
                        data.toJavaRDD()
                                .map(row -> (InternalRow) new SparkRow(writeBuilder.rowType(), row))
                                .groupBy(new ComputeBucket(writeBuilder));
            }

            List<CommitMessage> committables =
                    grouped.mapValues(new WriteRecords(writeBuilder)).values()
                            .mapPartitions(SparkWrite::serializeCommitMessages).collect().stream()
                            .map(this::deserializeCommitMessage)
                            .collect(Collectors.toList());

            try (BatchTableCommit tableCommit =
                    ((InnerTableCommit) writeBuilder.newCommit()).withLock(lockFactory.create())) {
                tableCommit.commit(committables);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private boolean isDynamicBucketTable() {
        return table.isFileStoreTable()
                && ((FileStoreTable) table).bucketMode() == BucketMode.DYNAMIC;
    }

    private static Iterator<InternalRow> assignBucket(
            FileStoreTable fileStoreTable, BatchWriteBuilder writeBuilder, Iterator<Row> rows) {
        long targetBucketRowNumber = fileStoreTable.coreOptions().dynamicBucketTargetRowNum();
        List<InternalRow> dynamicBucketRows = new ArrayList<>();
        IndexFileHandler indexFileHandler = fileStoreTable.store().newIndexFileHandler();
        RowPartitionKeyExtractor rowPartitionKeyExtractor =
                new RowPartitionKeyExtractor(fileStoreTable.schema());
        Map<BinaryRow, PartitionIndex> partitionIndex = new HashMap<>();
        while (rows.hasNext()) {
            SparkRow internalRow = new SparkRow(writeBuilder.rowType(), rows.next());
            int hash = rowPartitionKeyExtractor.trimmedPrimaryKey(internalRow).hashCode();
            BinaryRow partition = rowPartitionKeyExtractor.partition(internalRow);
            PartitionIndex index =
                    partitionIndex.computeIfAbsent(
                            partition,
                            (p) ->
                                    PartitionIndex.loadIndex(
                                            indexFileHandler, p, targetBucketRowNumber));
            dynamicBucketRows.add(new DynamicBucketRow(internalRow, index.assign(hash)));
        }
        return dynamicBucketRows.iterator();
    }

    private static Iterator<byte[]> serializeCommitMessages(Iterator<List<CommitMessage>> iters) {
        List<byte[]> serialized = new ArrayList<>();
        CommitMessageSerializer innerSerializer = new CommitMessageSerializer();
        while (iters.hasNext()) {
            List<CommitMessage> commitMessages = iters.next();
            for (CommitMessage commitMessage : commitMessages) {
                try {
                    serialized.add(innerSerializer.serialize(commitMessage));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to serialize CommitMessage's object", e);
                }
            }
        }
        return serialized.iterator();
    }

    private CommitMessage deserializeCommitMessage(byte[] bytes) {
        try {
            return serializer.deserialize(serializer.getVersion(), bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize CommitMessage's object", e);
        }
    }

    private static class ComputeBucket implements Function<InternalRow, Integer> {

        private final BatchWriteBuilder writeBuilder;

        private transient BatchTableWrite lazyWriter;

        private ComputeBucket(BatchWriteBuilder writeBuilder) {
            this.writeBuilder = writeBuilder;
        }

        private BatchTableWrite computer() {
            if (lazyWriter == null) {
                lazyWriter = writeBuilder.newWrite();
            }
            return lazyWriter;
        }

        @Override
        public Integer call(InternalRow row) {
            return computer().getBucket(row);
        }
    }

    private static class WriteRecords
            implements Function<Iterable<InternalRow>, List<CommitMessage>> {

        private final BatchWriteBuilder writeBuilder;

        private WriteRecords(BatchWriteBuilder writeBuilder) {
            this.writeBuilder = writeBuilder;
        }

        @Override
        public List<CommitMessage> call(Iterable<InternalRow> iterables) throws Exception {
            try (BatchTableWrite write = writeBuilder.newWrite()) {
                for (InternalRow row : iterables) {
                    write.write(row);
                }
                return write.prepareCommit();
            }
        }
    }
}
