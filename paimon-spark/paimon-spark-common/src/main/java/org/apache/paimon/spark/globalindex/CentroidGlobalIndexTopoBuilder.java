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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.VectorIndexShardMeta;
import org.apache.paimon.globalindex.VectorIndexUtils;
import org.apache.paimon.globalindex.VectorTrainingModel;
import org.apache.paimon.globalindex.VectorTrainingModelIO;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import org.apache.spark.HashPartitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.util.TaskCompletionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Single-partition full build: shared training, centroid shuffle, then one atomic index commit. */
public class CentroidGlobalIndexTopoBuilder implements GlobalIndexTopologyBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(CentroidGlobalIndexTopoBuilder.class);

    @Override
    public List<CommitMessage> buildIndex(
            SparkSession spark,
            DataSourceV2Relation relation,
            PartitionPredicate partitionPredicate,
            FileStoreTable table,
            String indexType,
            RowType readType,
            DataField indexField,
            Options options)
            throws IOException {
        checkArgument(
                org.apache.paimon.globalindex.VectorIndexUtils.isVectorIndexType(indexType),
                "Unsupported centroid index type: %s",
                indexType);
        checkArgument(
                table.coreOptions().dataEvolutionEnabled(),
                "Centroid sharding requires a Data Evolution table.");
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        if (snapshot == null) {
            return Collections.emptyList();
        }
        List<ManifestEntry> entries =
                table.store()
                        .newScan()
                        .withSnapshot(snapshot)
                        .withPartitionFilter(partitionPredicate)
                        .plan()
                        .files();
        List<BinaryRow> partitions =
                entries.stream()
                        .map(ManifestEntry::partition)
                        .distinct()
                        .collect(Collectors.toList());
        checkArgument(
                partitions.size() <= 1,
                "Centroid sharding supports a full build of exactly one partition.");
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }
        BinaryRow partition = partitions.get(0);
        List<IndexedSplit> splits =
                GlobalIndexBuilderUtils.createShardIndexedSplits(
                        table, entries, DefaultGlobalIndexTopoBuilder.rowsPerShard(options));
        if (splits.isEmpty()) {
            return Collections.emptyList();
        }
        List<Range> ranges = new ArrayList<>();
        List<byte[]> serializedSplits = new ArrayList<>();
        for (IndexedSplit split : splits) {
            ranges.addAll(split.rowRanges());
            serializedSplits.add(InstantiationUtil.serializeObject(split));
        }
        ranges = Range.sortAndMergeOverlap(ranges, true);
        BuildContext context =
                new BuildContext(
                        table,
                        readType,
                        indexField,
                        options,
                        partition,
                        snapshot.id(),
                        new Range(ranges.get(0).from, ranges.get(ranges.size() - 1).to),
                        indexType);
        byte[] contextBytes = InstantiationUtil.serializeObject(context);
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<InternalRow> rows =
                sc.parallelize(
                                serializedSplits,
                                DefaultGlobalIndexTopoBuilder.parallelism(splits.size(), options))
                        .mapPartitions(
                                input -> new VectorRows(BuildContext.restore(contextBytes), input));
        VectorGlobalIndexer indexer = context.indexer();
        byte[] payload = train(sc, contextBytes, rows, indexer, context.getter());
        int nlist;
        try (VectorTrainingModel model = indexer.restoreTrainingModel(payload)) {
            nlist = model.nlist();
        }
        String modelId = UUID.randomUUID().toString();
        Broadcast<byte[]> broadcast = sc.broadcast(payload);
        try {
            int parallelism = DefaultGlobalIndexTopoBuilder.parallelism(nlist, options);
            JavaPairRDD<Integer, RoutedVector> assigned =
                    rows.mapPartitionsToPair(
                            input ->
                                    route(
                                            BuildContext.restore(contextBytes),
                                            input,
                                            broadcast.value()));
            // Seed every centroid so empty lists also have an explicit, verifiable shard file.
            List<Tuple2<Integer, RoutedVector>> emptyLists = new ArrayList<>();
            for (int c = 0; c < nlist; c++) {
                emptyLists.add(new Tuple2<>(c, new RoutedVector(-1, null)));
            }
            List<byte[]> results =
                    assigned.union(sc.parallelizePairs(emptyLists, parallelism))
                            .repartitionAndSortWithinPartitions(new HashPartitioner(parallelism))
                            .mapPartitions(
                                    input ->
                                            writeShards(
                                                    BuildContext.restore(contextBytes),
                                                    input,
                                                    broadcast.value(),
                                                    modelId))
                            .collect();
            List<CommitMessage> messages = CommitMessageSerializer.deserializeAll(results);
            GlobalIndexFileReadWrite io = context.io();
            String modelFile =
                    "vector-" + indexType + "-global-index-training-model-" + modelId + ".index";
            try (PositionOutputStream out = io.newOutputStream(modelFile)) {
                VectorTrainingModelIO.write(out, payload, modelId);
            }
            IndexFileMeta modelMeta =
                    GlobalIndexBuilderUtils.toIndexFileMetas(
                                    table.fileIO(),
                                    table.store().pathFactory().globalIndexFileFactory(),
                                    table.coreOptions(),
                                    context.envelope,
                                    Collections.singletonList(indexField),
                                    indexType,
                                    Collections.singletonList(
                                            new ResultEntry(
                                                    modelFile,
                                                    0,
                                                    VectorIndexShardMeta.routing(
                                                                    modelId, nlist, ranges)
                                                            .serialize())),
                                    context.sourceMeta())
                            .get(0);
            modelMeta =
                    new IndexFileMeta(
                            modelMeta.indexType(),
                            modelMeta.fileName(),
                            modelMeta.fileSize(),
                            0,
                            null,
                            modelMeta.externalPath(),
                            modelMeta.globalIndexMeta(),
                            IndexFileMeta.ROUTING_MODEL);
            messages.add(context.message(Collections.singletonList(modelMeta)));
            List<IndexFileMeta> old =
                    GlobalIndexBuilderUtils.currentIndexEntries(
                                    table,
                                    snapshot,
                                    indexType,
                                    Collections.singletonList(indexField),
                                    partitionPredicate)
                            .stream()
                            .map(IndexManifestEntry::indexFile)
                            .collect(Collectors.toList());
            if (!old.isEmpty()) {
                messages.add(
                        new CommitMessageImpl(
                                        partition,
                                        0,
                                        null,
                                        DataIncrement.deleteIndexIncrement(old),
                                        CompactIncrement.emptyIncrement())
                                .withCheckFromSnapshot(snapshot.id()));
            }
            return messages;
        } finally {
            broadcast.destroy();
        }
    }

    @Override
    public List<CommitMessage> buildIndex(
            SparkSession spark,
            DataSourceV2Relation relation,
            PartitionPredicate partitionPredicate,
            FileStoreTable table,
            String indexType,
            RowType readType,
            DataField indexField,
            List<DataField> extraFields,
            Options options)
            throws IOException {
        checkArgument(extraFields.isEmpty(), "Centroid sharding supports one vector column.");
        return buildIndex(
                spark,
                relation,
                partitionPredicate,
                table,
                indexType,
                readType,
                indexField,
                options);
    }

    private static byte[] train(
            JavaSparkContext sc,
            byte[] contextBytes,
            JavaRDD<InternalRow> rows,
            VectorGlobalIndexer indexer,
            InternalRow.FieldGetter getter) {
        // Count actual non-null rows at the pinned snapshot. Row-ID spans can contain
        // gaps, nulls and deletions, so a split's planned row count is insufficient.
        List<Long> counts =
                rows.mapPartitions(
                                input -> {
                                    long count = 0;
                                    while (input.hasNext()) {
                                        input.next();
                                        count++;
                                    }
                                    return Collections.singletonList(count).iterator();
                                })
                        .collect();
        long[] offsets = new long[counts.size() + 1];
        for (int i = 0; i < counts.size(); i++) {
            offsets[i + 1] = Math.addExact(offsets[i], counts.get(i));
        }
        long total = offsets[counts.size()];
        long[] positions = indexer.planGlobalTrainingSample(total);
        int sampleSize = positions.length;
        LOG.info(
                "Sampling {} of {} non-null vectors across {} Spark input partitions for centroid training",
                sampleSize,
                total,
                counts.size());
        Broadcast<CentroidTrainingSample> plan =
                sc.broadcast(new CentroidTrainingSample(positions));
        try {
            JavaRDD<Tuple2<Integer, float[]>> samples =
                    rows.mapPartitionsWithIndex(
                            (partition, input) ->
                                    plan.value()
                                            .select(
                                                    input,
                                                    getter,
                                                    offsets[partition],
                                                    offsets[partition + 1]),
                            false);
            return JavaPairRDD.fromJavaRDD(samples)
                    .repartitionAndSortWithinPartitions(new HashPartitioner(1))
                    .values()
                    .mapPartitions(
                            input -> {
                                try (VectorTrainingModel model =
                                        BuildContext.restore(contextBytes)
                                                .indexer()
                                                .trainGlobalModel(
                                                        total,
                                                        CentroidTrainingSample.batches(
                                                                input, sampleSize))) {
                                    return Collections.singletonList(model.serialize()).iterator();
                                }
                            })
                    .collect()
                    .get(0);
        } finally {
            plan.destroy();
        }
    }

    private static Iterator<Tuple2<Integer, RoutedVector>> route(
            BuildContext context, Iterator<InternalRow> input, byte[] payload) {
        VectorTrainingModel model = context.indexer().restoreTrainingModel(payload);
        closeOnTaskCompletion(model);
        InternalRow.FieldGetter getter = context.getter();
        int idPosition = context.rowIdPosition();
        return Iterators.transform(
                input,
                row -> {
                    float[] vector = VectorIndexUtils.toFloatArray(getter.getFieldOrNull(row));
                    return new Tuple2<>(
                            model.assignCentroid(vector),
                            new RoutedVector(row.getLong(idPosition), vector));
                });
    }

    private static void closeOnTaskCompletion(AutoCloseable resource) {
        TaskContext.get()
                .addTaskCompletionListener(
                        (TaskCompletionListener)
                                ignored -> {
                                    try {
                                        resource.close();
                                    } catch (Exception e) {
                                        throw new RuntimeException(
                                                "Could not close centroid build resource", e);
                                    }
                                });
    }

    private static Iterator<byte[]> writeShards(
            BuildContext context,
            Iterator<Tuple2<Integer, RoutedVector>> rows,
            byte[] payload,
            String modelId)
            throws Exception {
        List<byte[]> commits = new ArrayList<>();
        try (VectorTrainingModel model = context.indexer().restoreTrainingModel(payload)) {
            GlobalIndexSingleColumnWriter writer = null;
            Integer centroid = null;
            try {
                while (rows.hasNext()) {
                    Tuple2<Integer, RoutedVector> row = rows.next();
                    if (!row._1().equals(centroid)) {
                        if (writer != null) {
                            commits.add(context.finish(writer));
                            close(writer);
                        }
                        centroid = row._1();
                        writer = model.createCentroidWriter(context.io(), centroid, modelId);
                    }
                    if (row._2().vector != null) {
                        writer.write(row._2().vector, row._2().id);
                    }
                }
                if (writer != null) {
                    commits.add(context.finish(writer));
                }
            } finally {
                close(writer);
            }
        }
        return commits.iterator();
    }

    private static void close(Object resource) throws IOException {
        if (resource instanceof Closeable) {
            ((Closeable) resource).close();
        }
    }

    private static final class RoutedVector implements Serializable {
        private static final long serialVersionUID = 1L;
        private final long id;
        private final float[] vector;

        RoutedVector(long id, float[] vector) {
            this.id = id;
            this.vector = vector;
        }
    }

    private static final class BuildContext implements Serializable {
        private static final long serialVersionUID = 1L;
        private final FileStoreTable table;
        private final RowType readType;
        private final DataField field;
        private final Options options;
        private final BinaryRow partition;
        private final long snapshotId;
        private final Range envelope;
        private final String indexType;

        BuildContext(
                FileStoreTable table,
                RowType readType,
                DataField field,
                Options options,
                BinaryRow partition,
                long snapshotId,
                Range envelope,
                String indexType) {
            this.table = table;
            this.readType = readType;
            this.field = field;
            this.options = options;
            this.partition = partition;
            this.snapshotId = snapshotId;
            this.envelope = envelope;
            this.indexType = indexType;
        }

        static BuildContext restore(byte[] bytes) throws IOException, ClassNotFoundException {
            return InstantiationUtil.deserializeObject(
                    bytes, CentroidGlobalIndexTopoBuilder.class.getClassLoader());
        }

        VectorGlobalIndexer indexer() {
            return (VectorGlobalIndexer)
                    GlobalIndexer.create(indexType, Collections.singletonList(field), options);
        }

        GlobalIndexFileReadWrite io() {
            return new GlobalIndexFileReadWrite(
                    table.fileIO(), table.store().pathFactory().globalIndexFileFactory());
        }

        int rowIdPosition() {
            return readType.getFieldIndex(SpecialFields.ROW_ID.name());
        }

        InternalRow.FieldGetter getter() {
            return InternalRow.createFieldGetter(
                    field.type(), readType.getFieldIndex(field.name()));
        }

        boolean contains(IndexedSplit split, long id) {
            for (Range range : split.rowRanges()) {
                if (id >= range.from && id <= range.to) {
                    return true;
                }
            }
            return false;
        }

        CloseableIterator<InternalRow> rows(IndexedSplit split) throws IOException {
            return table.newReadBuilder()
                    .withReadType(readType)
                    .newRead()
                    .createReader(split)
                    .toCloseableIterator();
        }

        byte[] sourceMeta() {
            return new DataEvolutionIndexSourceMeta(snapshotId).serialize();
        }

        CommitMessageImpl message(List<IndexFileMeta> files) {
            return new CommitMessageImpl(
                            partition,
                            0,
                            null,
                            DataIncrement.indexIncrement(files),
                            CompactIncrement.emptyIncrement())
                    .withCheckFromSnapshot(snapshotId);
        }

        byte[] finish(GlobalIndexSingleColumnWriter writer) throws IOException {
            List<IndexFileMeta> files =
                    GlobalIndexBuilderUtils.toIndexFileMetas(
                            table.fileIO(),
                            table.store().pathFactory().globalIndexFileFactory(),
                            table.coreOptions(),
                            envelope,
                            Collections.singletonList(field),
                            indexType,
                            writer.finish(),
                            sourceMeta());
            return new CommitMessageSerializer().serialize(message(files));
        }
    }

    private static final class VectorRows implements Iterator<InternalRow>, Closeable {
        private final BuildContext context;
        private final Iterator<byte[]> splits;
        private final int vectorPosition;
        private final int idPosition;
        private CloseableIterator<InternalRow> rows;
        private IndexedSplit split;
        private InternalRow next;
        private boolean closed;

        VectorRows(BuildContext context, Iterator<byte[]> splits) {
            this.context = context;
            this.splits = splits;
            this.vectorPosition = context.readType.getFieldIndex(context.field.name());
            this.idPosition = context.rowIdPosition();
            closeOnTaskCompletion(this);
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            if (closed) {
                return false;
            }
            try {
                while (true) {
                    if (rows == null || !rows.hasNext()) {
                        if (rows != null) {
                            rows.close();
                            rows = null;
                        }
                        if (!splits.hasNext()) {
                            close();
                            return false;
                        }
                        split =
                                InstantiationUtil.deserializeObject(
                                        splits.next(),
                                        CentroidGlobalIndexTopoBuilder.class.getClassLoader());
                        rows = context.rows(split);
                        continue;
                    }
                    InternalRow row = rows.next();
                    long id = row.getLong(idPosition);
                    if (!context.contains(split, id)) {
                        continue;
                    }
                    if (row.isNullAt(vectorPosition)) {
                        continue;
                    }
                    next = row;
                    return true;
                }
            } catch (Exception e) {
                try {
                    close();
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw new RuntimeException("Cannot read centroid build input.", e);
            }
        }

        @Override
        public InternalRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            InternalRow result = next;
            next = null;
            return result;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                try {
                    if (rows != null) {
                        rows.close();
                    }
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        }
    }
}
