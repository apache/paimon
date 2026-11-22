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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.GlobalIndexColumnUpdateAction;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.globalindex.CentroidIndexMeta;
import org.apache.paimon.globalindex.CentroidVectorGlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.VectorIndexUtils;
import org.apache.paimon.globalindex.VectorTrainingModel;
import org.apache.paimon.globalindex.bitmap.CentroidBitmapIndex;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.MutableObjectIteratorAdapter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.ThreadPoolUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;
import org.apache.paimon.shade.guava30.com.google.common.collect.PeekingIterator;

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

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Build a router and its shards for each contiguous range, then commit all files together. */
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
        return buildIndex(
                spark,
                relation,
                partitionPredicate,
                table,
                indexType,
                readType,
                indexField,
                Collections.emptyList(),
                options);
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
        CentroidVectorGlobalIndexer indexer = createIndexer(indexType, indexField, options);
        checkArgument(
                table.coreOptions().dataEvolutionEnabled(),
                "Centroid sharding requires a Data Evolution table.");
        checkArgument(
                extraFields.isEmpty()
                        || table.coreOptions().globalIndexColumnUpdateAction()
                                != GlobalIndexColumnUpdateAction.IGNORE,
                "Centroid bitmap fields require THROW_ERROR or DROP_PARTITION_INDEX.");
        for (DataField extraField : extraFields) {
            KeySerializer.create(extraField.type());
        }
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
        List<IndexedSplit> splits =
                GlobalIndexBuilderUtils.createShardIndexedSplits(
                        table, entries, DefaultGlobalIndexTopoBuilder.rowsPerShard(options));
        Map<BinaryRow, List<Pair<Range, IndexedSplit>>> partitionSplits = new HashMap<>();
        for (IndexedSplit split : splits) {
            // createShardIndexedSplits clips each task to a single physical row-ID range.
            partitionSplits
                    .computeIfAbsent(split.dataSplit().partition(), key -> new ArrayList<>())
                    .add(Pair.of(split.rowRanges().get(0), split));
        }
        Map<BinaryRow, Map<Range, List<IndexedSplit>>> groups =
                GlobalIndexBuilderUtils.groupSplitsByRange(partitionSplits);
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        List<Pair<BuildContext, List<IndexedSplit>>> ranges = new ArrayList<>();
        for (Map.Entry<BinaryRow, Map<Range, List<IndexedSplit>>> partition : groups.entrySet()) {
            for (Map.Entry<Range, List<IndexedSplit>> group : partition.getValue().entrySet()) {
                BuildContext context =
                        new BuildContext(
                                table,
                                readType,
                                indexField,
                                extraFields,
                                options,
                                partition.getKey(),
                                snapshot.id(),
                                table.schema().id(),
                                group.getKey(),
                                indexType);
                ranges.add(Pair.of(context, group.getValue()));
            }
        }
        List<CommitMessage> messages = new ArrayList<>();
        if (!ranges.isEmpty()) {
            int budget = DefaultGlobalIndexTopoBuilder.parallelism(Integer.MAX_VALUE, options);
            // Bound both the number of driver-side training plans and concurrent Spark tasks.
            int driverSlots =
                    Math.min(sc.defaultParallelism(), Runtime.getRuntime().availableProcessors());
            int workers = Math.min(ranges.size(), Math.min(driverSlots, budget));
            // Give larger input ranges their share of the slots before scheduling tiny ones.
            ranges.sort((a, b) -> Integer.compare(b.getRight().size(), a.getRight().size()));
            ExecutorService pool =
                    ThreadPoolUtils.createCachedThreadPool(workers, "centroid-build");
            try {
                for (int start = 0; start < ranges.size(); ) {
                    long splitsInBatch = 0;
                    int end = start;
                    while (end < ranges.size() && end - start < workers) {
                        int splitCount = ranges.get(end).getRight().size();
                        if ((long) budget * splitCount < splitsInBatch + splitCount) {
                            // This range would get less than one slot. Start it in the next batch.
                            break;
                        }
                        splitsInBatch += splitCount;
                        end++;
                    }
                    final long totalSplits = splitsInBatch;
                    try (ThreadPoolUtils.CloseableBatchIterator<CommitMessage> built =
                            ThreadPoolUtils.sequentialBatchedExecuteCloseable(
                                    pool,
                                    range -> {
                                        int slots =
                                                (int)
                                                        ((long) budget
                                                                * range.getRight().size()
                                                                / totalSplits);
                                        try {
                                            return buildRange(
                                                    sc,
                                                    range.getLeft(),
                                                    range.getRight(),
                                                    indexer,
                                                    slots);
                                        } catch (IOException e) {
                                            throw new UncheckedIOException(e);
                                        }
                                    },
                                    ranges.subList(start, end),
                                    workers)) {
                        built.forEachRemaining(messages::add);
                    }
                    start = end;
                }
            } catch (UncheckedIOException e) {
                throw e.getCause();
            } finally {
                pool.shutdown();
            }
        }
        Map<BinaryRow, List<IndexFileMeta>> previous =
                table.store().newIndexFileHandler().scan(snapshot, indexType).stream()
                        .filter(
                                entry ->
                                        partitionPredicate == null
                                                || partitionPredicate.test(entry.partition()))
                        .filter(
                                entry ->
                                        entry.indexFile().globalIndexMeta() != null
                                                && entry.indexFile()
                                                                .globalIndexMeta()
                                                                .indexFieldId()
                                                        == indexField.id())
                        .collect(
                                Collectors.groupingBy(
                                        IndexManifestEntry::partition,
                                        Collectors.mapping(
                                                IndexManifestEntry::indexFile,
                                                Collectors.toList())));
        for (Map.Entry<BinaryRow, List<IndexFileMeta>> entry : previous.entrySet()) {
            messages.add(
                    new CommitMessageImpl(
                                    entry.getKey(),
                                    0,
                                    null,
                                    DataIncrement.deleteIndexIncrement(entry.getValue()),
                                    CompactIncrement.emptyIncrement())
                            .withCheckFromSnapshot(snapshot.id()));
        }
        return messages;
    }

    private static List<CommitMessage> buildRange(
            JavaSparkContext sc,
            BuildContext context,
            List<IndexedSplit> splits,
            CentroidVectorGlobalIndexer indexer,
            int slots)
            throws IOException {
        List<byte[]> serializedSplits = new ArrayList<>(splits.size());
        for (IndexedSplit split : splits) {
            serializedSplits.add(InstantiationUtil.serializeObject(split));
        }
        byte[] contextBytes = InstantiationUtil.serializeObject(context);
        JavaRDD<byte[]> inputSplits =
                sc.parallelize(serializedSplits, Math.min(splits.size(), slots));
        JavaRDD<InternalRow> trainingRows =
                inputSplits.mapPartitions(
                        input -> {
                            BuildContext build = BuildContext.restore(contextBytes);
                            return new VectorRows(build, input, build.trainingReadType());
                        });
        byte[] payload =
                train(
                        sc,
                        contextBytes,
                        trainingRows,
                        indexer,
                        context.getter(context.trainingReadType()));
        String modelId = UUID.randomUUID().toString();
        if (payload == null) {
            // Record coverage so an all-null interval also satisfies index visibility checks.
            return Collections.singletonList(
                    writeRouter(context, indexer, new byte[0], modelId, 0));
        }
        int nlist;
        try (VectorTrainingModel model = indexer.restoreTrainingModel(payload)) {
            nlist = model.nlist();
        }
        Broadcast<byte[]> broadcast = sc.broadcast(payload);
        try {
            int parallelism = Math.min(nlist, slots);
            JavaRDD<InternalRow> routedRows =
                    context.hasBitmaps()
                            ? inputSplits.mapPartitions(
                                    input -> {
                                        BuildContext build = BuildContext.restore(contextBytes);
                                        return new VectorRows(build, input, build.readType);
                                    })
                            : trainingRows;
            // Keep the shuffle payload compatible with strict Spark Kryo registration.
            JavaPairRDD<Integer, Tuple2<Long, Tuple2<float[], byte[]>>> assigned =
                    routedRows.mapPartitionsToPair(
                            input ->
                                    route(
                                            BuildContext.restore(contextBytes),
                                            input,
                                            broadcast.value()));
            List<byte[]> results =
                    assigned.repartitionAndSortWithinPartitions(new HashPartitioner(parallelism))
                            .mapPartitionsWithIndex(
                                    (partition, input) ->
                                            writeShards(
                                                    BuildContext.restore(contextBytes),
                                                    input,
                                                    partition,
                                                    parallelism,
                                                    nlist,
                                                    modelId),
                                    false)
                            .collect();
            List<CommitMessage> messages = CommitMessageSerializer.deserializeAll(results);
            messages.add(writeRouter(context, indexer, payload, modelId, nlist));
            return messages;
        } finally {
            broadcast.destroy();
        }
    }

    private static CommitMessage writeRouter(
            BuildContext context,
            CentroidVectorGlobalIndexer indexer,
            byte[] payload,
            String modelId,
            int nlist)
            throws IOException {
        GlobalIndexFileReadWrite io = context.io();
        String modelFile =
                "vector-"
                        + context.indexType
                        + "-global-index-training-model-"
                        + modelId
                        + ".index";
        IndexFileMeta modelMeta =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                                context.table.fileIO(),
                                context.table.store().pathFactory().globalIndexFileFactory(),
                                context.table.coreOptions(),
                                context.rowRange,
                                context.indexFields,
                                context.indexType,
                                Collections.singletonList(
                                        CentroidIndexMeta.router(
                                                indexer.writeRoutingModel(
                                                        io, modelFile, payload, modelId),
                                                modelId,
                                                nlist,
                                                context.hasBitmaps())),
                                context.sourceMeta())
                        .get(0);
        return context.message(Collections.singletonList(modelMeta));
    }

    @Nullable
    private static byte[] train(
            JavaSparkContext sc,
            byte[] contextBytes,
            JavaRDD<InternalRow> rows,
            CentroidVectorGlobalIndexer indexer,
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
        if (total == 0) {
            return null;
        }
        long[] positions = indexer.planGlobalTrainingSample(total);
        int sampleSize = positions.length;
        LOG.info(
                "Sampling {} of {} non-null vectors across {} Spark input partitions for centroid training",
                sampleSize,
                total,
                counts.size());
        Broadcast<Tuple2<long[], int[]>> plan =
                sc.broadcast(CentroidTrainingSample.plan(positions));
        try {
            JavaRDD<Tuple2<Integer, float[]>> samples =
                    rows.mapPartitionsWithIndex(
                            (partition, input) ->
                                    CentroidTrainingSample.select(
                                            plan.value(),
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

    private static Iterator<Tuple2<Integer, Tuple2<Long, Tuple2<float[], byte[]>>>> route(
            BuildContext context, Iterator<InternalRow> input, byte[] payload) {
        VectorTrainingModel model = context.indexer().restoreTrainingModel(payload);
        closeOnTaskCompletion(model);
        InternalRow.FieldGetter getter = context.getter(context.readType);
        int idPosition = context.readType.getFieldIndex(SpecialFields.ROW_ID.name());
        List<InternalRow.FieldGetter> extraGetters = new ArrayList<>();
        for (DataField field : context.extraFields()) {
            extraGetters.add(
                    InternalRow.createFieldGetter(
                            field.type(), context.readType.getFieldIndex(field.name())));
        }
        InternalRowSerializer serializer =
                new InternalRowSerializer(new RowType(context.extraFields()));
        DataOutputSerializer buffer = new DataOutputSerializer(128);
        GenericRow extras = new GenericRow(extraGetters.size());
        return Iterators.transform(
                input,
                row -> {
                    float[] vector = VectorIndexUtils.toFloatArray(getter.getFieldOrNull(row));
                    byte[] keys = null;
                    if (!extraGetters.isEmpty()) {
                        for (int i = 0; i < extraGetters.size(); i++) {
                            extras.setField(i, extraGetters.get(i).getFieldOrNull(row));
                        }
                        try {
                            buffer.clear();
                            serializer.serialize(extras, buffer);
                            keys = buffer.getCopyOfBuffer();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                    return new Tuple2<>(
                            model.assignCentroid(vector),
                            new Tuple2<>(
                                    row.getLong(idPosition) - context.rowRange.from,
                                    new Tuple2<>(vector, keys)));
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
            Iterator<Tuple2<Integer, Tuple2<Long, Tuple2<float[], byte[]>>>> rows,
            int partition,
            int parallelism,
            int nlist,
            String modelId)
            throws Exception {
        List<byte[]> commits = new ArrayList<>();
        PeekingIterator<Tuple2<Integer, Tuple2<Long, Tuple2<float[], byte[]>>>> input =
                Iterators.peekingIterator(rows);
        CentroidVectorGlobalIndexer indexer = context.indexer();
        GlobalIndexFileReadWrite io = context.io();
        // HashPartitioner assigns nonnegative integer keys by modulo. Emit empty shards here
        // as well, without a second set of input tasks to shuffle sentinel records.
        for (int centroid = partition; centroid < nlist; centroid += parallelism) {
            GlobalIndexSingleColumnWriter writer = indexer.createCentroidWriter(io);
            try (BitmapSorters bitmapSorters =
                    context.hasBitmaps() && input.hasNext() && input.peek()._1() == centroid
                            ? new BitmapSorters(context)
                            : null) {
                while (input.hasNext() && input.peek()._1() == centroid) {
                    Tuple2<Long, Tuple2<float[], byte[]>> row = input.next()._2();
                    writer.write(row._2()._1(), row._1());
                    if (bitmapSorters != null) {
                        bitmapSorters.add(row._1(), row._2()._2());
                    }
                }
                List<ResultEntry> files = new ArrayList<>();
                for (ResultEntry file : writer.finish()) {
                    files.add(
                            CentroidIndexMeta.shard(file, modelId, centroid, context.hasBitmaps()));
                }
                if (bitmapSorters != null) {
                    files.add(
                            CentroidIndexMeta.scalarIndex(
                                    bitmapSorters.finish(context, io), modelId, centroid));
                }
                commits.add(context.finish(files));
            } finally {
                close(writer);
            }
        }
        return commits.iterator();
    }

    private static final class BitmapSorters implements AutoCloseable {
        private final IOManager ioManager;
        private final List<BinaryExternalSortBuffer> buffers = new ArrayList<>();
        private final List<InternalRow.FieldGetter> getters = new ArrayList<>();
        private final InternalRowSerializer serializer;
        private final DataInputDeserializer input = new DataInputDeserializer();
        private long count;

        BitmapSorters(BuildContext context) throws Exception {
            List<DataField> fields = context.extraFields();
            this.serializer = new InternalRowSerializer(new RowType(fields));
            CoreOptions options = new CoreOptions(context.options);
            long memory =
                    Math.max(3L * options.pageSize(), options.writeBufferSize() / fields.size());
            this.ioManager = IOManager.create(System.getProperty("java.io.tmpdir"));
            try {
                for (int i = 0; i < fields.size(); i++) {
                    DataField field = fields.get(i);
                    getters.add(InternalRow.createFieldGetter(field.type(), i));
                    buffers.add(
                            BinaryExternalSortBuffer.create(
                                    ioManager,
                                    RowType.of(field, SpecialFields.ROW_ID),
                                    new int[] {0},
                                    memory,
                                    options.pageSize(),
                                    options.localSortMaxNumFileHandles(),
                                    options.spillCompressOptions(),
                                    options.writeBufferSpillDiskSize()));
                }
            } catch (Exception | Error failure) {
                try {
                    close();
                } catch (Exception cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }

        void add(long rowId, byte[] keys) throws IOException {
            input.setBuffer(keys);
            InternalRow row = serializer.deserialize(input);
            for (int i = 0; i < buffers.size(); i++) {
                Object value = getters.get(i).getFieldOrNull(row);
                checkArgument(
                        buffers.get(i).write(GenericRow.of(value, rowId)),
                        "Centroid bitmap sort buffer could not accept a row.");
            }
            count++;
        }

        ResultEntry finish(BuildContext context, GlobalIndexFileReadWrite files)
                throws IOException {
            List<DataField> fields = context.extraFields();
            try (CentroidBitmapIndex.Writer writer =
                    new CentroidBitmapIndex.Writer(files, context.schemaId, fields.size(), count)) {
                for (int i = 0; i < fields.size(); i++) {
                    DataField field = fields.get(i);
                    BinaryExternalSortBuffer buffer = buffers.get(i);
                    Iterator<InternalRow> rows =
                            new MutableObjectIteratorAdapter<>(
                                    buffer.sortedIterator(), new BinaryRow(2));
                    InternalRow.FieldGetter getter = InternalRow.createFieldGetter(field.type(), 0);
                    writer.writeColumn(
                            field,
                            new CoreOptions(context.options)
                                    .globalIndexOptions("bitmap", field.name()),
                            bitmap -> {
                                while (rows.hasNext()) {
                                    InternalRow row = rows.next();
                                    Object value = getter.getFieldOrNull(row);
                                    bitmap.write(
                                            value instanceof BinaryString
                                                    ? ((BinaryString) value).copy()
                                                    : value,
                                            row.getLong(1));
                                }
                            });
                    buffer.clear();
                }
                return writer.finish();
            }
        }

        @Override
        public void close() throws Exception {
            try {
                buffers.forEach(BinaryExternalSortBuffer::clear);
            } finally {
                ioManager.close();
            }
        }
    }

    private static void close(Object resource) throws IOException {
        if (resource instanceof Closeable) {
            ((Closeable) resource).close();
        }
    }

    static CentroidVectorGlobalIndexer createIndexer(
            String indexType, DataField field, Options options) {
        GlobalIndexer indexer =
                GlobalIndexer.create(indexType, Collections.singletonList(field), options);
        checkArgument(
                indexer instanceof CentroidVectorGlobalIndexer,
                "Index '%s' does not support centroid sharding.",
                indexType);
        return (CentroidVectorGlobalIndexer) indexer;
    }

    private static final class BuildContext implements Serializable {
        private static final long serialVersionUID = 1L;
        private final FileStoreTable table;
        private final RowType readType;
        private final DataField field;
        private final List<DataField> indexFields;
        private final Options options;
        private final BinaryRow partition;
        private final long snapshotId;
        private final long schemaId;
        private final Range rowRange;
        private final String indexType;

        BuildContext(
                FileStoreTable table,
                RowType readType,
                DataField field,
                List<DataField> extraFields,
                Options options,
                BinaryRow partition,
                long snapshotId,
                long schemaId,
                Range rowRange,
                String indexType) {
            this.table = table;
            this.readType = readType;
            this.field = field;
            this.indexFields = new ArrayList<>(1 + extraFields.size());
            this.indexFields.add(field);
            this.indexFields.addAll(extraFields);
            this.options = options;
            this.partition = partition;
            this.snapshotId = snapshotId;
            this.schemaId = schemaId;
            this.rowRange = rowRange;
            this.indexType = indexType;
        }

        List<DataField> extraFields() {
            return indexFields.subList(1, indexFields.size());
        }

        boolean hasBitmaps() {
            return indexFields.size() > 1;
        }

        static BuildContext restore(byte[] bytes) throws IOException, ClassNotFoundException {
            return InstantiationUtil.deserializeObject(
                    bytes, CentroidGlobalIndexTopoBuilder.class.getClassLoader());
        }

        CentroidVectorGlobalIndexer indexer() {
            return createIndexer(indexType, field, options);
        }

        GlobalIndexFileReadWrite io() {
            return new GlobalIndexFileReadWrite(
                    table.fileIO(), table.store().pathFactory().globalIndexFileFactory());
        }

        RowType trainingReadType() {
            return hasBitmaps() ? RowType.of(field, SpecialFields.ROW_ID) : readType;
        }

        InternalRow.FieldGetter getter(RowType projection) {
            return InternalRow.createFieldGetter(
                    field.type(), projection.getFieldIndex(field.name()));
        }

        boolean contains(IndexedSplit split, long id) {
            for (Range range : split.rowRanges()) {
                if (id >= range.from && id <= range.to) {
                    return true;
                }
            }
            return false;
        }

        CloseableIterator<InternalRow> rows(IndexedSplit split, RowType projection)
                throws IOException {
            return table.newReadBuilder()
                    .withReadType(projection)
                    .newRead()
                    .createReader(split)
                    .toCloseableIterator();
        }

        byte[] sourceMeta() {
            return new DataEvolutionIndexSourceMeta(snapshotId, schemaId).serialize();
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

        byte[] finish(List<ResultEntry> entries) throws IOException {
            List<IndexFileMeta> files =
                    GlobalIndexBuilderUtils.toIndexFileMetas(
                            table.fileIO(),
                            table.store().pathFactory().globalIndexFileFactory(),
                            table.coreOptions(),
                            rowRange,
                            indexFields,
                            indexType,
                            entries,
                            sourceMeta());
            return new CommitMessageSerializer().serialize(message(files));
        }
    }

    private static final class VectorRows implements Iterator<InternalRow>, Closeable {
        private final BuildContext context;
        private final Iterator<byte[]> splits;
        private final RowType projection;
        private final int vectorPosition;
        private final int idPosition;
        private CloseableIterator<InternalRow> rows;
        private IndexedSplit split;
        private InternalRow next;
        private boolean closed;

        VectorRows(BuildContext context, Iterator<byte[]> splits, RowType projection) {
            this.context = context;
            this.splits = splits;
            this.projection = projection;
            this.vectorPosition = projection.getFieldIndex(context.field.name());
            this.idPosition = projection.getFieldIndex(SpecialFields.ROW_ID.name());
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
                        rows = context.rows(split, projection);
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
