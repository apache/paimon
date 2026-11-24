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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.CentroidIndexFile;
import org.apache.paimon.globalindex.CentroidIndexMeta;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.bitmap.CentroidBitmapIndex;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;
import org.apache.paimon.shade.guava30.com.google.common.collect.PeekingIterator;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Tuple2;

import static org.apache.paimon.spark.globalindex.CentroidGlobalIndexTopoBuilder.closeOnTaskCompletion;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Adds Bitmap columns to centroid ranges using their persisted vector positions. */
public final class CentroidBitmapFieldAppender {

    private CentroidBitmapFieldAppender() {}

    public static List<CommitMessage> build(
            SparkSession spark,
            FileStoreTable table,
            String indexType,
            DataField vectorField,
            List<DataField> requestedFields,
            @Nullable PartitionPredicate partitionFilter)
            throws IOException {
        checkArgument(
                table.coreOptions().dataEvolutionEnabled(),
                "Centroid Bitmap fields require a Data Evolution table.");
        checkArgument(!requestedFields.isEmpty(), "Specify at least one extra field.");
        requestedFields.forEach(field -> KeySerializer.create(field.type()));

        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        List<IndexManifestEntry> entries =
                snapshot == null
                        ? Collections.emptyList()
                        : table.store()
                                .newIndexFileHandler()
                                .scan(
                                        snapshot,
                                        entry -> {
                                            IndexFileMeta file = entry.indexFile();
                                            GlobalIndexMeta meta = file.globalIndexMeta();
                                            return indexType.equals(file.indexType())
                                                    && meta != null
                                                    && meta.indexFieldId() == vectorField.id()
                                                    && CentroidIndexMeta.hasBitmapFields(meta)
                                                    && (partitionFilter == null
                                                            || partitionFilter.test(
                                                                    entry.partition()));
                                        });
        Map<Pair<BinaryRow, Range>, List<IndexManifestEntry>> groups = new HashMap<>();
        for (IndexManifestEntry entry : entries) {
            groups.computeIfAbsent(
                            Pair.of(
                                    entry.partition(),
                                    entry.indexFile().globalIndexMeta().rowRange()),
                            ignored -> new ArrayList<>())
                    .add(entry);
        }
        checkArgument(
                !groups.isEmpty(),
                "No centroid %s index with Bitmap fields on column '%s' in the selected partitions.",
                indexType,
                vectorField.name());

        List<CommitMessage> messages = new ArrayList<>();
        List<BitmapRange> ranges = new ArrayList<>();
        int bitmapCount = 0;
        Set<DataField> projectedFields = new LinkedHashSet<>();
        Map<Long, RowType> buildSchemas = new HashMap<>();
        RowType rowType = table.rowType();
        buildSchemas.put(table.schema().id(), rowType);
        Map<Integer, DataField> requestedById =
                requestedFields.stream()
                        .collect(Collectors.toMap(DataField::id, Function.identity()));
        long snapshotId = snapshot.id();
        byte[] sourceMeta =
                new DataEvolutionIndexSourceMeta(snapshotId, table.schema().id()).serialize();
        IndexPathFactory paths = table.store().pathFactory().globalIndexFileFactory();
        for (List<IndexManifestEntry> group : groups.values()) {
            GlobalIndexMeta meta = group.get(0).indexFile().globalIndexMeta();
            int[] oldFieldIds = meta.extraFieldIds();
            List<DataField> addedFields =
                    requestedFields.stream()
                            .filter(
                                    field ->
                                            Arrays.stream(oldFieldIds)
                                                    .noneMatch(id -> id == field.id()))
                            .collect(Collectors.toList());
            if (addedFields.isEmpty()) {
                for (IndexManifestEntry entry : group) {
                    IndexFileMeta file = entry.indexFile();
                    if (!CentroidIndexMeta.isScalarIndex(file)) {
                        continue;
                    }
                    CentroidIndexFile bitmap =
                            (CentroidIndexFile) CentroidIndexMeta.file(file, paths);
                    DataField field = requestedById.get(bitmap.fieldId());
                    if (field != null) {
                        checkBitmapFieldType(table, buildSchemas, file, field);
                    }
                }
                continue;
            }
            checkArgument(
                    table.coreOptions().globalIndexColumnUpdateAction()
                            != CoreOptions.GlobalIndexColumnUpdateAction.IGNORE,
                    "Centroid Bitmap fields require THROW_ERROR or DROP_PARTITION_INDEX.");

            List<DataField> fields = new ArrayList<>();
            fields.add(vectorField);
            for (int id : oldFieldIds) {
                checkArgument(
                        rowType.containsField(id),
                        "Existing Bitmap field ID %s is missing. Rebuild the centroid index.",
                        id);
                fields.add(rowType.getField(id));
            }
            fields.addAll(addedFields);
            int[] extraFieldIds = fields.stream().skip(1).mapToInt(DataField::id).toArray();
            List<IndexFileMeta> replaced = new ArrayList<>(group.size());
            List<IndexFileMeta> retained = new ArrayList<>(group.size());
            Map<Integer, CentroidIndexFile> bitmaps = new HashMap<>();
            long vectorCount = 0;
            long bitmapSchemaId = -1;
            int nlist = -1;
            for (IndexManifestEntry entry : group) {
                IndexFileMeta file = entry.indexFile();
                GlobalIndexMeta current = file.globalIndexMeta();
                checkState(
                        Arrays.equals(oldFieldIds, current.extraFieldIds()),
                        "Mixed centroid Bitmap fields.");
                replaced.add(file);
                CentroidIndexFile descriptor =
                        (CentroidIndexFile) CentroidIndexMeta.file(file, paths);
                if (descriptor.routingModel()) {
                    checkState(nlist == -1, "Duplicate centroid router.");
                    nlist = descriptor.nlist();
                } else if (descriptor.scalarIndex()) {
                    int fieldId = descriptor.fieldId();
                    checkState(
                            Arrays.stream(oldFieldIds).anyMatch(id -> id == fieldId)
                                    && bitmaps.put(fieldId, descriptor) == null,
                            "Unexpected centroid Bitmap file for field %s.",
                            fieldId);
                    long schemaId =
                            checkBitmapFieldType(
                                    table, buildSchemas, file, rowType.getField(fieldId));
                    checkState(
                            bitmapSchemaId == -1 || bitmapSchemaId == schemaId,
                            "Mixed centroid Bitmap schemas.");
                    bitmapSchemaId = schemaId;
                } else {
                    vectorCount = Math.addExact(vectorCount, file.rowCount());
                }
                retained.add(
                        new IndexFileMeta(
                                file.indexType(),
                                file.fileName(),
                                file.fileSize(),
                                file.rowCount(),
                                new GlobalIndexMeta(
                                        current.rowRangeStart(),
                                        current.rowRangeEnd(),
                                        current.indexFieldId(),
                                        extraFieldIds,
                                        current.indexMeta(),
                                        sourceMeta),
                                file.externalPath()));
            }
            IndexManifestEntry first = group.get(0);
            checkState(nlist >= 0, "Missing centroid routing model.");
            if (vectorCount == 0) {
                checkState(bitmaps.isEmpty(), "Unexpected Bitmap for an empty centroid range.");
            } else {
                checkState(
                        nlist > 0 && bitmaps.size() == oldFieldIds.length,
                        "Centroid Bitmap files do not cover the range's fields.");
                for (CentroidIndexFile bitmap : bitmaps.values()) {
                    checkState(
                            bitmap.rowCount() == vectorCount,
                            "Centroid Bitmap row count differs from the total vector count.");
                }
                ranges.add(
                        new BitmapRange(
                                first,
                                bitmaps.get(oldFieldIds[0]),
                                fields,
                                oldFieldIds.length,
                                nlist,
                                bitmapCount));
                bitmapCount += addedFields.size();
                projectedFields.addAll(addedFields);
            }
            messages.add(
                    message(first.partition(), first.bucket(), retained, replaced, snapshotId));
        }

        if (!ranges.isEmpty()) {
            List<ManifestEntry> dataFiles =
                    table.store()
                            .newScan()
                            .withSnapshot(snapshot)
                            .withPartitionFilter(partitionFilter)
                            .plan()
                            .files();
            List<Range> indexedRanges =
                    ranges.stream()
                            .map(range -> range.range)
                            .distinct()
                            .collect(Collectors.toList());
            Options options = new Options(table.options());
            List<IndexedSplit> splits =
                    GlobalIndexBuilderUtils.createShardIndexedSplits(
                            table,
                            dataFiles,
                            DefaultGlobalIndexTopoBuilder.rowsPerShard(options),
                            indexedRanges);
            messages.addAll(
                    writeBitmaps(
                            spark,
                            table,
                            options,
                            snapshotId,
                            sourceMeta,
                            ranges,
                            bitmapCount,
                            splits,
                            new RowType(new ArrayList<>(projectedFields))));
        }
        return messages;
    }

    private static long checkBitmapFieldType(
            FileStoreTable table,
            Map<Long, RowType> buildSchemas,
            IndexFileMeta file,
            DataField field) {
        long schemaId = DataEvolutionIndexSourceMeta.fromIndexFile(file).sourceSchemaId();
        checkState(schemaId >= 0, "Missing centroid Bitmap build schema.");
        RowType buildType =
                buildSchemas.computeIfAbsent(
                        schemaId, id -> table.schemaManager().schema(id).logicalRowType());
        checkArgument(
                buildType.containsField(field.id())
                        && field.type().equalsIgnoreNullable(buildType.getField(field.id()).type()),
                "Bitmap field '%s' has changed type. Rebuild the centroid index.",
                field.name());
        return schemaId;
    }

    private static List<CommitMessage> writeBitmaps(
            SparkSession spark,
            FileStoreTable table,
            Options options,
            long snapshotId,
            byte[] sourceMeta,
            List<BitmapRange> ranges,
            int bitmapCount,
            List<IndexedSplit> splits,
            RowType keyType)
            throws IOException {
        checkState(!splits.isEmpty(), "No source rows for centroid Bitmap fields.");
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        int writeTasks = DefaultGlobalIndexTopoBuilder.parallelism(bitmapCount, options);
        int readTasks = DefaultGlobalIndexTopoBuilder.parallelism(splits.size(), options);
        List<byte[]> splitBytes = new ArrayList<>(splits.size());
        for (IndexedSplit split : splits) {
            splitBytes.add(InstantiationUtil.serializeObject(split));
        }
        List<Integer> ordinals =
                IntStream.range(0, ranges.size()).boxed().collect(Collectors.toList());
        int[] outputRanges = new int[bitmapCount];
        int[] rangeOutputOffsets = new int[ranges.size() + 1];
        int[] outputFieldPositions = new int[bitmapCount];
        for (int i = 0; i < ranges.size(); i++) {
            BitmapRange range = ranges.get(i);
            int outputId = range.firstOutputId;
            rangeOutputOffsets[i] = outputId;
            for (DataField field : range.addedFields()) {
                outputRanges[outputId] = i;
                outputFieldPositions[outputId] = keyType.getFieldIndex(field.name());
                outputId++;
            }
        }
        rangeOutputOffsets[ranges.size()] = bitmapCount;
        Broadcast<Tuple2<byte[], int[]>> rangePlans = sc.broadcast(serializeRanges(ranges));
        try {
            JavaPairRDD<Long, Tuple2<Integer, Long>> membership =
                    sc.parallelize(ordinals, Math.min(writeTasks, ranges.size()))
                            .mapPartitionsToPair(
                                    ids -> {
                                        Tuple2<byte[], int[]> plans = rangePlans.value();
                                        GlobalIndexFileReadWrite files = indexFiles(table);
                                        return Iterators.concat(
                                                Iterators.transform(
                                                        ids,
                                                        id ->
                                                                rangePlan(plans, id)
                                                                        .rows(files, id)));
                                    });
            JavaPairRDD<Long, byte[]> values =
                    sc.parallelize(splitBytes, readTasks)
                            .mapPartitionsToPair(ids -> new ScalarRows(table, keyType, ids));

            // Read each source split once and repartition its values by Bitmap output file.
            JavaPairRDD<Integer, Tuple2<Long, byte[]>> rowsByBitmap =
                    values.join(membership, Math.max(writeTasks, readTasks))
                            .mapPartitionsToPair(
                                    rows ->
                                            bitmapValues(
                                                    keyType,
                                                    rangeOutputOffsets,
                                                    outputFieldPositions,
                                                    rows));
            List<byte[]> outputs =
                    rowsByBitmap
                            .repartitionAndSortWithinPartitions(new HashPartitioner(writeTasks))
                            .mapPartitions(
                                    rows -> {
                                        Tuple2<byte[], int[]> plans = rangePlans.value();
                                        GlobalIndexFileReadWrite files = indexFiles(table);
                                        PeekingIterator<Tuple2<Integer, Tuple2<Long, byte[]>>>
                                                input = Iterators.peekingIterator(rows);
                                        List<byte[]> finished = new ArrayList<>();
                                        while (input.hasNext()) {
                                            int id = input.peek()._1();
                                            finished.add(
                                                    rangePlan(plans, outputRanges[id])
                                                            .write(
                                                                    table,
                                                                    files,
                                                                    id,
                                                                    input,
                                                                    snapshotId,
                                                                    sourceMeta));
                                        }
                                        return finished.iterator();
                                    })
                            .collect();
            checkState(outputs.size() == bitmapCount, "Missing source rows for a centroid Bitmap.");
            return CommitMessageSerializer.deserializeAll(outputs);
        } finally {
            rangePlans.destroy();
        }
    }

    private static Iterator<Tuple2<Integer, Tuple2<Long, byte[]>>> bitmapValues(
            RowType keyType,
            int[] rangeOutputOffsets,
            int[] outputFieldPositions,
            Iterator<Tuple2<Long, Tuple2<byte[], Tuple2<Integer, Long>>>> rows) {
        InternalRowSerializer rowSerializer = new InternalRowSerializer(keyType);
        DataInputDeserializer input = new DataInputDeserializer();
        InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[keyType.getFieldCount()];
        KeySerializer[] serializers = new KeySerializer[keyType.getFieldCount()];
        for (int i = 0; i < getters.length; i++) {
            getters[i] = InternalRow.createFieldGetter(keyType.getTypeAt(i), i);
            serializers[i] = KeySerializer.create(keyType.getTypeAt(i));
        }
        return Iterators.concat(
                Iterators.transform(
                        rows,
                        row -> {
                            Tuple2<Integer, Long> position = row._2()._2();
                            int rangeId = position._1();
                            int from = rangeOutputOffsets[rangeId];
                            int to = rangeOutputOffsets[rangeId + 1];
                            List<Tuple2<Integer, Tuple2<Long, byte[]>>> values =
                                    new ArrayList<>(to - from);
                            input.setBuffer(row._2()._1());
                            InternalRow keys;
                            try {
                                keys = rowSerializer.deserialize(input);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                            for (int outputId = from; outputId < to; outputId++) {
                                int column = outputFieldPositions[outputId];
                                Object value = getters[column].getFieldOrNull(keys);
                                byte[] bytes =
                                        value == null ? null : serializers[column].serialize(value);
                                values.add(
                                        new Tuple2<>(outputId, new Tuple2<>(position._2(), bytes)));
                            }
                            return values.iterator();
                        }));
    }

    private static CommitMessage message(
            BinaryRow partition,
            int bucket,
            List<IndexFileMeta> added,
            List<IndexFileMeta> deleted,
            long snapshotId) {
        return new CommitMessageImpl(
                        partition,
                        bucket,
                        null,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                added,
                                deleted),
                        CompactIncrement.emptyIncrement())
                .withCheckFromSnapshot(snapshotId);
    }

    private static GlobalIndexFileReadWrite indexFiles(FileStoreTable table) {
        return new GlobalIndexFileReadWrite(
                table.fileIO(), table.store().pathFactory().globalIndexFileFactory());
    }

    private static Tuple2<byte[], int[]> serializeRanges(List<BitmapRange> ranges)
            throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        int[] offsets = new int[ranges.size() + 1];
        for (int i = 0; i < ranges.size(); i++) {
            payload.write(InstantiationUtil.serializeObject(ranges.get(i)));
            offsets[i + 1] = payload.size();
        }
        return new Tuple2<>(payload.toByteArray(), offsets);
    }

    private static BitmapRange rangePlan(Tuple2<byte[], int[]> plans, int id) {
        int[] offsets = plans._2();
        try {
            return InstantiationUtil.deserializeObject(
                    new ByteArrayInputStream(
                            plans._1(), offsets[id], offsets[id + 1] - offsets[id]),
                    CentroidBitmapFieldAppender.class.getClassLoader());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot deserialize centroid Bitmap range plan.", e);
        }
    }

    private static <T> T deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        return InstantiationUtil.deserializeObject(
                bytes, CentroidBitmapFieldAppender.class.getClassLoader());
    }

    private static final class BitmapRange implements Serializable {

        private static final long serialVersionUID = 1L;

        private final BinaryRow partition;
        private final int bucket;
        private final Range range;
        private final String indexType;
        private final int nlist;
        private final Path path;
        private final long fileSize;
        private final long rowCount;
        private final byte[] bitmapMetadata;
        private final List<DataField> fields;
        private final int oldFieldCount;
        private final int firstOutputId;

        private BitmapRange(
                IndexManifestEntry entry,
                CentroidIndexFile descriptor,
                List<DataField> fields,
                int oldFieldCount,
                int nlist,
                int firstOutputId) {
            this.partition = entry.partition();
            this.bucket = entry.bucket();
            this.range = entry.indexFile().globalIndexMeta().rowRange();
            this.indexType = entry.indexFile().indexType();
            this.nlist = nlist;
            this.path = descriptor.filePath();
            this.fileSize = descriptor.fileSize();
            this.rowCount = descriptor.rowCount();
            this.bitmapMetadata = descriptor.metadata();
            this.fields = fields;
            this.oldFieldCount = oldFieldCount;
            this.firstOutputId = firstOutputId;
        }

        private List<DataField> addedFields() {
            return fields.subList(oldFieldCount + 1, fields.size());
        }

        private Iterator<Tuple2<Long, Tuple2<Integer, Long>>> rows(
                GlobalIndexFileReadWrite files, int id) {
            GlobalIndexIOMeta bitmap =
                    new GlobalIndexIOMeta(path, fileSize, rowCount, bitmapMetadata);
            long rangeSize = range.count();
            try {
                return Iterators.transform(
                        CentroidBitmapIndex.rowIds(files, bitmap, fields.get(1)).iterator(),
                        encoded -> {
                            checkState(
                                    encoded >= 0 && encoded / rangeSize < nlist,
                                    "Invalid centroid Bitmap position: %s.",
                                    encoded);
                            long rowId = range.from + encoded % rangeSize;
                            return new Tuple2<>(rowId, new Tuple2<>(id, encoded));
                        });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private byte[] write(
                FileStoreTable table,
                GlobalIndexFileReadWrite files,
                int id,
                PeekingIterator<Tuple2<Integer, Tuple2<Long, byte[]>>> rows,
                long snapshotId,
                byte[] sourceMeta)
                throws Exception {
            DataField field = addedFields().get(id - firstOutputId);
            List<ResultEntry> entries =
                    CentroidGlobalIndexTopoBuilder.writeBitmap(
                            table.coreOptions(), files, rows, id, field, Function.identity());
            checkState(
                    entries.size() == 1 && entries.get(0).rowCount() == rowCount,
                    "Source rows do not cover the centroid Bitmap for field '%s'.",
                    field.name());
            List<IndexFileMeta> added =
                    GlobalIndexBuilderUtils.toIndexFileMetas(
                            table.fileIO(),
                            table.coreOptions(),
                            range,
                            fields,
                            indexType,
                            entries,
                            sourceMeta);
            return new CommitMessageSerializer()
                    .serialize(
                            message(partition, bucket, added, Collections.emptyList(), snapshotId));
        }
    }

    private static final class ScalarRows implements Iterator<Tuple2<Long, byte[]>>, AutoCloseable {

        private final Iterator<byte[]> splits;
        private final TableRead read;
        private final int rowIdPosition;
        private final ProjectedRow keys;
        private final InternalRowSerializer serializer;
        private final DataOutputSerializer output = new DataOutputSerializer(128);

        private CloseableIterator<InternalRow> rows = CloseableIterator.empty();

        private ScalarRows(FileStoreTable table, RowType keyType, Iterator<byte[]> splits) {
            this.splits = splits;
            this.read =
                    table.newReadBuilder()
                            .withReadType(SpecialFields.rowTypeWithRowId(keyType))
                            .newRead();
            this.rowIdPosition = keyType.getFieldCount();
            this.keys = ProjectedRow.from(IntStream.range(0, rowIdPosition).toArray());
            this.serializer = new InternalRowSerializer(keyType);
            closeOnTaskCompletion(this);
        }

        @Override
        public boolean hasNext() {
            try {
                while (!rows.hasNext()) {
                    close();
                    if (!splits.hasNext()) {
                        return false;
                    }
                    IndexedSplit split = deserialize(splits.next());
                    rows = read.createReader(split).toCloseableIterator();
                }
                return true;
            } catch (Exception e) {
                throw new RuntimeException("Cannot read centroid Bitmap source rows.", e);
            }
        }

        @Override
        public Tuple2<Long, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            InternalRow row = rows.next();
            try {
                output.clear();
                serializer.serialize(keys.replaceRow(row), output);
                return new Tuple2<>(row.getLong(rowIdPosition), output.getCopyOfBuffer());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() throws Exception {
            CloseableIterator<InternalRow> current = rows;
            rows = CloseableIterator.empty();
            current.close();
        }
    }
}
