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
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/** To remove wrong duplicated manifests. */
public class CompactDuplicatedManifestsAction extends TableActionBase {

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

        SingleOutputStreamOperator<byte[]> source =
                env.fromData(
                                binaryPartitions.stream()
                                        .map(BinaryRow::toBytes)
                                        .collect(Collectors.toList()),
                                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                        .name("Compact Duplicated Manifests Source")
                        .forceNonParallel();

        TypeInformation<ManifestEntry> entryType = TypeInformation.of(ManifestEntry.class);
        SingleOutputStreamOperator<List<ManifestEntry>> worker =
                source.transform(
                        "Compact Duplicated Manifests Worker",
                        Types.LIST(entryType),
                        new WorkerOperator(fileStoreTable));
        if (parallelism != null) {
            worker = worker.setParallelism(Math.min(parallelism, binaryPartitions.size()));
        }

        worker.transform(
                        "Compact Duplicated Manifests Committer",
                        Types.STRING,
                        new CommitOperator(fileStoreTable))
                .forceNonParallel()
                .sinkTo(new DiscardingSink<>())
                .name("end")
                .setParallelism(1);
    }

    private static class WorkerOperator
            extends BoundedOneInputOperator<byte[], List<ManifestEntry>> {

        private static final long serialVersionUID = 1L;

        private final FileStoreTable table;

        private transient List<BinaryRow> partitions;

        private WorkerOperator(FileStoreTable table) {
            this.table = table;
        }

        @Override
        public void open() {
            partitions = new ArrayList<>();
        }

        @Override
        public void processElement(StreamRecord<byte[]> record) {
            byte[] bytes = record.getValue();
            BinaryRow partition = new BinaryRow(table.schema().partitionKeys().size());
            partition.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
            partitions.add(partition);
        }

        @Override
        public void endInput() {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                throw new RuntimeException("Expected latest snapshot existed.");
            }

            ManifestList manifestList = table.store().manifestListFactory().create();
            List<ManifestFileMeta> allManifests = manifestList.readDataManifests(latestSnapshot);

            // TODO: handle non partitioned table
            PartitionPredicate predicate =
                    PartitionPredicate.fromMultiple(
                            table.schema().logicalPartitionType(), partitions);
            List<ManifestFileMeta> partitionFilteredManifests = new ArrayList<>();
            for (ManifestFileMeta file : allManifests) {
                if (predicate.test(
                        file.numAddedFiles() + file.numDeletedFiles(),
                        file.partitionStats().minValues(),
                        file.partitionStats().maxValues(),
                        file.partitionStats().nullCounts())) {
                    partitionFilteredManifests.add(file);
                }
            }

            ManifestFile manifestFile = table.store().manifestFileFactory().create();
            List<ManifestEntry> toMerge = new ArrayList<>();
            for (ManifestFileMeta file : partitionFilteredManifests) {
                for (ManifestEntry entry : manifestFile.read(file.fileName(), file.fileSize())) {
                    if (partitions.contains(entry.partition())) {
                        toMerge.add(entry);
                    }
                }
            }

            List<ManifestEntry> merged = new ArrayList<>(FileEntry.mergeEntries(toMerge));
            output.collect(new StreamRecord<>(merged));
        }
    }

    private static class CommitOperator
            extends BoundedOneInputOperator<List<ManifestEntry>, String> {

        private static final long serialVersionUID = 1L;

        private final FileStoreTable table;

        private transient ManifestFile manifestFile;
        private transient List<ManifestFileMeta> merged;

        private CommitOperator(FileStoreTable table) {
            this.table = table;
        }

        @Override
        public void open() {
            manifestFile = table.store().manifestFileFactory().create();
            merged = new ArrayList<>();
        }

        @Override
        public void processElement(StreamRecord<List<ManifestEntry>> record) throws Exception {
            List<ManifestEntry> entries = record.getValue();
            RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                    manifestFile.createRollingWriter();
            try {
                writer.write(entries);
            } catch (Exception e) {
                writer.abort();
                throw e;
            }
            writer.close();
            merged.addAll(writer.result());
        }

        @Override
        public void endInput() throws Exception {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                throw new RuntimeException("Expected latest snapshot existed.");
            }

            ManifestList manifestList = table.store().manifestListFactory().create();
            Pair<String, Long> baseManifestList = manifestList.write(merged);
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
