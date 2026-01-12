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

package org.apache.paimon.flink.action;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestFileMetaSerializer;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.emptyList;

/** To remove wrong duplicated manifests. */
public class CompactDuplicatedManifestsAction extends TableActionBase {

    private static final OutputTag<InternalRow> UNPROCESSED_MANIFESTS_OUTPUT =
            new OutputTag<>(
                    "unprocessed-manifests-output",
                    InternalTypeInfo.fromRowType(ManifestFileMeta.SCHEMA));

    @Nullable private List<Map<String, String>> partitions;
    @Nullable private Integer parallelism;

    public CompactDuplicatedManifestsAction(
            String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
    }

    public CompactDuplicatedManifestsAction withPartitions(
            @Nullable List<Map<String, String>> partitions) {
        this.partitions = partitions;
        return this;
    }

    public CompactDuplicatedManifestsAction withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    @Override
    public void build() throws Exception {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<BinaryRow> binaryPartitions = new ArrayList<>();
        if (partitions != null) {
            RowType partitionType = fileStoreTable.schema().logicalPartitionType();
            String defaultPartitionName = fileStoreTable.coreOptions().partitionDefaultName();
            InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
            for (Map<String, String> partition : partitions) {
                GenericRow row =
                        InternalRowPartitionComputer.convertSpecToInternalRow(
                                partition, partitionType, defaultPartitionName);
                binaryPartitions.add(serializer.toBinaryRow(row).copy());
            }
        } else {
            binaryPartitions = fileStoreTable.newScan().listPartitions();
        }

        DataStreamSource<Long> source = env.fromSequence(0, 1);

        SingleOutputStreamOperator<InternalRow> picker =
                source.transform(
                                "Compact Duplicated Manifests Picker",
                                InternalTypeInfo.fromRowType(ManifestFileMeta.SCHEMA),
                                new PickOperator(fileStoreTable, binaryPartitions))
                        .forceNonParallel();

        SingleOutputStreamOperator<InternalRow> merger =
                picker.transform(
                        "Compact Duplicated Manifests Merger",
                        InternalTypeInfo.fromRowType(ManifestFileMeta.SCHEMA),
                        new MergerOperator(fileStoreTable, binaryPartitions));
        if (parallelism != null) {
            merger = merger.setParallelism(Math.min(parallelism, binaryPartitions.size()));
        }

        DataStream<InternalRow> unprocessed = picker.getSideOutput(UNPROCESSED_MANIFESTS_OUTPUT);
        DataStream<InternalRow> toBeCommitted = merger.union(unprocessed);

        toBeCommitted
                .transform(
                        "Compact Duplicated Manifests Committer",
                        Types.STRING,
                        new CommitOperator(fileStoreTable))
                .forceNonParallel()
                .sinkTo(new DiscardingSink<>())
                .name("end")
                .setParallelism(1);
    }

    private static class PickOperator extends BoundedOneInputOperator<Long, InternalRow> {

        private static final long serialVersionUID = 1L;

        private final FileStoreTable table;
        private final List<BinaryRow> specifiedPartitions;

        private PickOperator(FileStoreTable table, List<BinaryRow> specifiedPartitions) {
            this.table = table;
            this.specifiedPartitions = specifiedPartitions;
        }

        @Override
        public void processElement(StreamRecord<Long> record) {}

        @Override
        public void endInput() {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                throw new RuntimeException("Expected latest snapshot existed.");
            }

            // TODO: handle non partitioned table
            PartitionPredicate predicate =
                    PartitionPredicate.fromMultiple(
                            table.schema().logicalPartitionType(), specifiedPartitions);
            ManifestList manifestList = table.store().manifestListFactory().create();
            List<ManifestFileMeta> allManifests = manifestList.readDataManifests(latestSnapshot);

            ManifestFileMetaSerializer serializer = new ManifestFileMetaSerializer();
            for (ManifestFileMeta file : allManifests) {
                if (predicate.test(
                        file.numAddedFiles() + file.numDeletedFiles(),
                        file.partitionStats().minValues(),
                        file.partitionStats().maxValues(),
                        file.partitionStats().nullCounts())) {
                    output.collect(new StreamRecord<>(serializer.convertTo(file)));
                } else {
                    output.collect(
                            UNPROCESSED_MANIFESTS_OUTPUT,
                            new StreamRecord<>(serializer.convertTo(file)));
                }
            }
        }
    }

    private static class MergerOperator extends BoundedOneInputOperator<InternalRow, InternalRow> {

        private static final long serialVersionUID = 1L;

        private final FileStoreTable table;
        private final List<BinaryRow> specifiedPartitions;

        private transient ManifestFileMetaSerializer serializer;
        private transient List<ManifestFileMeta> manifestFileMetas;

        private MergerOperator(FileStoreTable table, List<BinaryRow> specifiedPartitions) {
            this.table = table;
            this.specifiedPartitions = specifiedPartitions;
        }

        @Override
        public void open() {
            serializer = new ManifestFileMetaSerializer();
            manifestFileMetas = new ArrayList<>();
        }

        @Override
        public void processElement(StreamRecord<InternalRow> record) {
            manifestFileMetas.add(
                    serializer.convertFrom(serializer.getVersion(), record.getValue()));
        }

        @Override
        public void endInput() throws Exception {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                throw new RuntimeException("Expected latest snapshot existed.");
            }

            ManifestFile manifestFile = table.store().manifestFileFactory().create();
            List<ManifestEntry> toMerge = new ArrayList<>();
            List<ManifestEntry> noMerge = new ArrayList<>();
            for (ManifestFileMeta file : manifestFileMetas) {
                for (ManifestEntry entry : manifestFile.read(file.fileName(), file.fileSize())) {
                    if (specifiedPartitions.contains(entry.partition())) {
                        toMerge.add(entry);
                    } else {
                        noMerge.add(entry);
                    }
                }
            }
            List<ManifestEntry> merged = new ArrayList<>(FileEntry.mergeEntries(toMerge));
            merged.addAll(noMerge);

            RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                    manifestFile.createRollingWriter();
            try {
                writer.write(merged);
            } catch (Exception e) {
                writer.abort();
                throw e;
            }
            writer.close();
            List<ManifestFileMeta> written = writer.result();
            for (ManifestFileMeta file : written) {
                output.collect(new StreamRecord<>(serializer.convertTo(file)));
            }
        }
    }

    private static class CommitOperator extends BoundedOneInputOperator<InternalRow, String> {

        private static final long serialVersionUID = 1L;

        private final FileStoreTable table;

        private transient ManifestFileMetaSerializer serializer;
        private transient List<ManifestFileMeta> toBeCommitted;

        private CommitOperator(FileStoreTable table) {
            this.table = table;
        }

        @Override
        public void open() {
            serializer = new ManifestFileMetaSerializer();
            toBeCommitted = new ArrayList<>();
        }

        @Override
        public void processElement(StreamRecord<InternalRow> record) {
            toBeCommitted.add(serializer.convertFrom(serializer.getVersion(), record.getValue()));
        }

        @Override
        public void endInput() throws Exception {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                throw new RuntimeException("Expected latest snapshot existed.");
            }

            ManifestList manifestList = table.store().manifestListFactory().create();
            Pair<String, Long> baseManifestList = manifestList.write(toBeCommitted);
            Pair<String, Long> deltaManifestList = manifestList.write(emptyList());

            // prepare snapshot file
            Snapshot newSnapshot =
                    new Snapshot(
                            latestSnapshot.id() + 1,
                            latestSnapshot.schemaId(),
                            baseManifestList.getLeft(),
                            baseManifestList.getRight(),
                            deltaManifestList.getLeft(),
                            deltaManifestList.getRight(),
                            null,
                            null,
                            latestSnapshot.indexManifest(),
                            "CompactDuplicatedManifests_" + UUID.randomUUID(),
                            Long.MAX_VALUE,
                            Snapshot.CommitKind.COMPACT,
                            System.currentTimeMillis(),
                            latestSnapshot.totalRecordCount(),
                            0L,
                            null,
                            latestSnapshot.watermark(),
                            latestSnapshot.statistics(),
                            latestSnapshot.properties(),
                            latestSnapshot.nextRowId());

            try (SnapshotCommit snapshotCommit =
                    table.catalogEnvironment().snapshotCommit(table.snapshotManager())) {
                snapshotCommit.commit(newSnapshot, Identifier.DEFAULT_MAIN_BRANCH, emptyList());
            }

            output.collect(new StreamRecord<>(newSnapshot.toString()));
        }
    }
}
