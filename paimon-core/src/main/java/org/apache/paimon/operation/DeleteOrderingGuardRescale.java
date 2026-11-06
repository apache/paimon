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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeleteOrderingGuardFile;
import org.apache.paimon.index.DeleteOrderingGuardReaderFactory;
import org.apache.paimon.index.DeleteOrderingGuardSerializer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.mergetree.compact.DeleteOrderingGuardReader;
import org.apache.paimon.operation.commit.DeleteOrderingGuardGeneration;
import org.apache.paimon.options.VectorIndexOptions;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Native rescale tasks for guarded tables. Visible rows alone cannot carry hidden bounds or
 * internal sequence ties. Tasks copy physical sorted records and metadata to the new key buckets;
 * one pinned overwrite publishes all replacements atomically.
 */
public class DeleteOrderingGuardRescale implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PrimaryKeyFileStoreTable source;
    private final PrimaryKeyFileStoreTable target;
    private final Snapshot snapshot;
    @Nullable private final PartitionPredicate partitions;
    private final int buckets;

    public static boolean isRequired(FileStoreTable table, Snapshot snapshot) {
        return table instanceof PrimaryKeyFileStoreTable
                && (table.coreOptions().deleteOrderingGuardsEnabled()
                        || DeleteOrderingGuardFile.mayContainGuards(snapshot));
    }

    public DeleteOrderingGuardRescale(
            PrimaryKeyFileStoreTable table,
            Snapshot snapshot,
            int buckets,
            @Nullable PartitionPredicate partitions) {
        checkArgument(buckets > 0, "Guard rescaling requires a positive fixed bucket count.");
        checkArgument(
                table.bucketMode() == BucketMode.HASH_FIXED,
                "Delete ordering guard rescaling supports fixed buckets only.");
        CoreOptions options = table.coreOptions();
        checkArgument(
                !options.deletionVectorsEnabled()
                        && !options.isChainTable()
                        && VectorIndexOptions.columns(options.toConfiguration()).isEmpty()
                        && !options.fullTextIndexEnabled()
                        && !options.fmIndexEnabled()
                        && options.btreeIndexColumns().isEmpty()
                        && options.bitmapIndexColumns().isEmpty()
                        && options.multiValueIndexColumns().isEmpty(),
                "Guard rescaling requires a native table without deletion vectors or primary-key indexes.");
        this.source = table;
        this.snapshot = snapshot;
        this.partitions = partitions;
        this.buckets = buckets;
        Map<String, String> targetOptions = new HashMap<>(table.options());
        targetOptions.put(CoreOptions.BUCKET.key(), String.valueOf(buckets));
        this.target = (PrimaryKeyFileStoreTable) table.copy(table.schema().copy(targetOptions));
    }

    /** One task per old bucket; coalesce split-level copies of the same guard references. */
    public List<DataSplit> plan() {
        SnapshotReader reader =
                source.newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withDeleteOrderingGuardBuckets()
                        .onlyReadRealBuckets();
        if (partitions != null) {
            reader.withPartitionFilter(partitions);
        }
        Map<Pair<BinaryRow, Integer>, List<DataSplit>> grouped = new LinkedHashMap<>();
        for (DataSplit split : reader.read().dataSplits()) {
            grouped.computeIfAbsent(
                            Pair.of(split.partition(), split.bucket()),
                            ignored -> new ArrayList<>())
                    .add(split);
        }
        List<DataSplit> tasks = new ArrayList<>();
        grouped.forEach(
                (bucket, splits) -> {
                    Set<DataFileMeta> data = new LinkedHashSet<>();
                    Set<IndexFileMeta> guards = new LinkedHashSet<>();
                    for (DataSplit split : splits) {
                        data.addAll(split.dataFiles());
                        guards.addAll(split.deleteOrderingGuards());
                    }
                    tasks.add(
                            DataSplit.builder()
                                    .withSnapshot(snapshot.id())
                                    .withPartition(bucket.getLeft())
                                    .withBucket(bucket.getRight())
                                    .withTotalBuckets(splits.get(0).totalBuckets())
                                    .withBucketPath(splits.get(0).bucketPath())
                                    .withDataFiles(new ArrayList<>(data))
                                    .withDeleteOrderingGuards(new ArrayList<>(guards))
                                    .rawConvertible(false)
                                    .build());
                });
        return tasks;
    }

    /** Streaming worker operation; outputs remain uncommitted until all tasks succeed. */
    public List<CommitMessage> rewrite(DataSplit split) throws Exception {
        checkArgument(split.snapshotId() == snapshot.id(), "Rescale input snapshot changed.");
        checkArgument(
                partitions == null || partitions.test(split.partition()),
                "Rescale input is outside the selected partitions.");
        RowType keyType =
                new RowType(
                        PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR.keyFields(
                                source.schema()));
        Projection bucketKey =
                CodeGenUtils.newProjection(
                        keyType,
                        source.schema().bucketKeys().stream()
                                .mapToInt(source.schema().trimmedPrimaryKeys()::indexOf)
                                .toArray());
        BucketFunction bucketFunction =
                BucketFunction.create(target.coreOptions(), target.schema().logicalBucketKeyType());
        KeyValueFileStore targetStore = target.store();
        KeyValueFileWriterFactory.Builder factories = targetStore.newWriterFactoryBuilder();
        KeyValueFileReaderFactory dataReader =
                source.store()
                        .newReaderFactoryBuilder()
                        .buildWithoutDeletionVector(split.partition(), split.bucket());
        Map<Integer, Output> outputs = new LinkedHashMap<>();
        List<RollingFileWriter<KeyValue, DataFileMeta>> dataWriters = new ArrayList<>();
        List<DeleteOrderingGuardFile.Writer> guardWriters = new ArrayList<>();
        try (CacheManager cache =
                new CacheManager(
                        source.coreOptions().lookupCacheMaxMemory(),
                        source.coreOptions().lookupCacheHighPrioPoolRatio())) {
            for (DataFileMeta file : split.dataFiles()) {
                // Each file is sorted. Filtering by a destination bucket preserves its ordering.
                // Use L0 because disjoint old buckets can have overlapping new key ranges.
                Map<Integer, RollingFileWriter<KeyValue, DataFileMeta>> writers =
                        new LinkedHashMap<>();
                try (RecordReaderIterator<KeyValue> rows =
                        new RecordReaderIterator<>(dataReader.createRecordReader(file))) {
                    while (rows.hasNext()) {
                        KeyValue row = rows.next();
                        int bucket = bucketFunction.bucket(bucketKey.apply(row.key()), buckets);
                        RollingFileWriter<KeyValue, DataFileMeta> writer = writers.get(bucket);
                        if (writer == null) {
                            writer =
                                    factories
                                            .build(split.partition(), bucket, target.coreOptions())
                                            .createRollingMergeTreeFileWriter(
                                                    0, FileSource.COMPACT);
                            writers.put(bucket, writer);
                            dataWriters.add(writer);
                        }
                        // Bypass ingestion sequence assignment and merge/pruning entirely.
                        writer.write(row);
                    }
                }
                for (Map.Entry<Integer, RollingFileWriter<KeyValue, DataFileMeta>> entry :
                        writers.entrySet()) {
                    entry.getValue().close();
                    outputs.computeIfAbsent(entry.getKey(), ignored -> new Output())
                            .data
                            .addAll(entry.getValue().result());
                }
            }
            if (!split.deleteOrderingGuards().isEmpty()) {
                DeleteOrderingGuardSerializer serializer =
                        new DeleteOrderingGuardSerializer(
                                target.schema().id(),
                                keyType,
                                target.rowType(),
                                target.coreOptions());
                DeleteOrderingGuardReaderFactory readers =
                        new DeleteOrderingGuardReaderFactory(
                                source.fileIO(),
                                new FileSystemSchemaManager(
                                        source.fileIO(), source.location(), source.currentBranch()),
                                source.coreOptions(),
                                keyType,
                                source.rowType(),
                                source.store().pathFactory(),
                                cache);
                Map<Integer, DeleteOrderingGuardFile.Writer> writers = new LinkedHashMap<>();
                try (DeleteOrderingGuardReader reader =
                        readers.open(
                                split.partition(),
                                split.bucket(),
                                split.deleteOrderingGuards(),
                                source.rowType(),
                                source.store().newKeyComparator())) {
                    List<KeyValue> bounds;
                    while ((bounds = reader.nextKey()) != null) {
                        int bucket =
                                bucketFunction.bucket(
                                        bucketKey.apply(bounds.get(0).key()), buckets);
                        DeleteOrderingGuardFile.Writer writer = writers.get(bucket);
                        if (writer == null) {
                            writer =
                                    new DeleteOrderingGuardFile(
                                                    target.fileIO(),
                                                    targetStore
                                                            .pathFactory()
                                                            .indexFileFactory(
                                                                    split.partition(), bucket),
                                                    serializer,
                                                    cache,
                                                    4096)
                                            .createWriter();
                            writers.put(bucket, writer);
                            guardWriters.add(writer);
                        }
                        for (KeyValue bound : bounds) {
                            writer.write(serializer.extract(bound));
                        }
                    }
                }
                for (Map.Entry<Integer, DeleteOrderingGuardFile.Writer> entry :
                        writers.entrySet()) {
                    IndexFileMeta replacement = entry.getValue().finish();
                    entry.getValue().close();
                    if (replacement != null) {
                        outputs.computeIfAbsent(entry.getKey(), ignored -> new Output())
                                .guards
                                .add(replacement);
                    }
                }
            }
            List<CommitMessage> messages = new ArrayList<>();
            outputs.forEach(
                    (bucket, output) ->
                            messages.add(
                                    new CommitMessageImpl(
                                            split.partition(),
                                            bucket,
                                            buckets,
                                            new DataIncrement(
                                                    output.data,
                                                    Collections.emptyList(),
                                                    Collections.emptyList(),
                                                    output.guards,
                                                    Collections.emptyList()),
                                            CompactIncrement.emptyIncrement(),
                                            null,
                                            DeleteOrderingGuardGeneration.forWriter(
                                                    snapshot,
                                                    split.partition(),
                                                    source.coreOptions()
                                                            .deleteOrderingGuardsEnabled()))));
            return messages;
        } catch (Throwable failure) {
            for (RollingFileWriter<KeyValue, DataFileMeta> writer : dataWriters) {
                try {
                    writer.abort();
                } catch (Throwable cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            for (DeleteOrderingGuardFile.Writer writer : guardWriters) {
                try {
                    writer.abort();
                } catch (Throwable cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    public void commit(String commitUser, long identifier, List<CommitMessage> messages) {
        ManifestCommittable committable = new ManifestCommittable(identifier);
        messages.forEach(committable::addFileCommittable);
        try (FileStoreCommitImpl commit = target.store().newCommit(commitUser, target)) {
            try {
                commit.rescale(snapshot, source.schema().id(), partitions, committable);
            } catch (RuntimeException failure) {
                // A lost publication response must never cause deletion of visible replacements.
                try {
                    if (!commit.filterCommitted(Collections.singletonList(committable)).isEmpty()) {
                        commit.abort(messages);
                    }
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }
    }

    /** Only abort outputs known not to have been published under this task's commit identity. */
    public void abort(String commitUser, long identifier, List<CommitMessage> messages) {
        ManifestCommittable committable = new ManifestCommittable(identifier);
        messages.forEach(committable::addFileCommittable);
        try (FileStoreCommitImpl commit = target.store().newCommit(commitUser, target)) {
            if (!commit.filterCommitted(Collections.singletonList(committable)).isEmpty()) {
                commit.abort(messages);
            }
        }
    }

    private static class Output {
        private final List<DataFileMeta> data = new ArrayList<>();
        private final List<IndexFileMeta> guards = new ArrayList<>();
    }
}
