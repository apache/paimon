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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionUpdateTimeExpireStrategy;
import org.apache.paimon.partition.PartitionValuesTimeExpireStrategy;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.PostponeFixedBucketWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.ScannerTestBase;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the independent partition maintenance scanner. */
public class PartitionEntryScannerTest extends ScannerTestBase {

    @ParameterizedTest
    @CsvSource({
        "primary,false",
        "primary,true",
        "append,false",
        "append,true",
        "evolution,false",
        "evolution,true"
    })
    void testMatchesFullScan(String mode, boolean sidecar) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, sidecar);
        if (mode.equals("evolution")) {
            options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
            options.set(CoreOptions.ROW_TRACKING_ENABLED, true);
            options.set(CoreOptions.BUCKET, -1);
        }
        tablePath = new Path(tablePath, "scanner");
        table = createFileStoreTable(mode.equals("primary"), options, tablePath);
        for (int commit = 0; commit < 3; commit++) {
            writeRows(commit * 10);
        }
        assertMatchesFullScan(manifests());
    }

    @ParameterizedTest
    @ValueSource(strings = {"update-time", "values-time"})
    void testDataEvolutionPartialUpdateAndExpiration(String expirationStrategy) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        options.set(CoreOptions.ROW_TRACKING_ENABLED, true);
        options.set(CoreOptions.BUCKET, -1);
        options.set(CoreOptions.PARTITION_TIMESTAMP_PATTERN, "$pt");
        options.set(CoreOptions.PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd");
        tablePath = new Path(tablePath, "partial-update");
        table = createFileStoreTable(false, options, tablePath);
        LocalDateTime initialTime = LocalDateTime.of(2023, 1, 1, 0, 0);
        int expiredPartition = 20230101;
        int updatedPartition = 20230103;

        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int partition : new int[] {expiredPartition, updatedPartition}) {
                write.write(GenericRow.of(partition, 1, 10L));
                write.write(GenericRow.of(partition, 2, 20L));
            }
            List<CommitMessage> messages = write.prepareCommit();
            setCreationTime(messages, initialTime);
            commit.commit(messages);
        }
        ManifestEntry original =
                table.store().newScan().plan().files().stream()
                        .filter(entry -> entry.partition().getInt(0) == updatedPartition)
                        .findFirst()
                        .get();
        long previousSequence = original.file().maxSequenceNumber();
        RowType writeType = table.rowType().project(Arrays.asList("pt", "b"));
        for (int version = 1; version <= 2; version++) {
            builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                    BatchTableCommit commit = builder.newCommit()) {
                write.write(GenericRow.of(updatedPartition, version * 100L));
                write.write(GenericRow.of(updatedPartition, version * 100L + 1));
                List<CommitMessage> messages = write.prepareCommit();
                for (CommitMessage message : messages) {
                    ((CommitMessageImpl) message)
                            .newFilesIncrement()
                            .newFiles()
                            .replaceAll(
                                    file -> file.assignFirstRowId(original.file().firstRowId()));
                }
                setCreationTime(messages, initialTime.plusDays(version));
                commit.commit(messages);
            }

            List<ManifestEntry> files = table.store().newScan().plan().files();
            assertThat(files).extracting(ManifestEntry::fileName).contains(original.fileName());
            DataFileMeta latest =
                    files.stream()
                            .filter(entry -> entry.partition().getInt(0) == updatedPartition)
                            .map(ManifestEntry::file)
                            .max(Comparator.comparingLong(DataFileMeta::maxSequenceNumber))
                            .get();
            assertThat(latest.writeCols()).contains("b").doesNotContain("a");
            assertThat(latest.firstRowId()).isEqualTo(original.file().firstRowId());
            assertThat(latest.rowCount()).isEqualTo(original.file().rowCount());
            assertThat(latest.maxSequenceNumber()).isGreaterThan(previousSequence);
            previousSequence = latest.maxSequenceNumber();
            assertMatchesFullScan(manifests());
        }

        PartitionExpireStrategy strategy =
                expirationStrategy.equals("update-time")
                        ? new PartitionUpdateTimeExpireStrategy(
                                table.coreOptions(), table.schema().logicalPartitionType())
                        : new PartitionValuesTimeExpireStrategy(
                                table.coreOptions(), table.schema().logicalPartitionType());
        LocalDateTime cutoff = initialTime.plusDays(1).plusHours(12);
        List<PartitionEntry> expected =
                strategy.selectExpiredPartitions(table.store().newScan(), cutoff);
        assertThat(expected)
                .extracting(entry -> entry.partition().getInt(0))
                .containsExactly(expiredPartition);
        NormalPartitionExpire expire =
                (NormalPartitionExpire)
                        table.store()
                                .newPartitionExpire(
                                        commitUser,
                                        table,
                                        Duration.ofDays(1),
                                        Duration.ZERO,
                                        strategy);
        assertThat(expire.expire(cutoff.plusDays(1), Long.MAX_VALUE))
                .containsExactly(Collections.singletonMap("pt", String.valueOf(expiredPartition)));
        assertMatchesFullScan(manifests());
        assertThat(getResult(table.newRead(), table.newScan().plan().splits()))
                .containsExactlyInAnyOrder("+I 20230103|1|200", "+I 20230103|2|201");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDeletedBucketLayout(boolean sidecar) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, -2);
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, sidecar);
        tablePath = new Path(tablePath, "scanner");
        table = createFileStoreTable(true, options, tablePath);
        PostponeFixedBucketWriteBuilder builder = table.newPostponeFixedBucketWriteBuilder();
        try (TableWriteImpl<?> write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.writeAndReturn(rowData(1, 1, 1L), 0, 4);
            commit.commit(write.prepareCommit());
        }
        builder = table.newPostponeFixedBucketWriteBuilder().withOverwrite(Collections.emptyMap());
        try (TableWriteImpl<?> write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.writeAndReturn(rowData(1, 2, 2L), 0, 2);
            commit.commit(write.prepareCommit());
        }
        List<ManifestFileMeta> manifests = manifests();
        assertThat(manifests.stream().mapToLong(ManifestFileMeta::numDeletedFiles).sum())
                .isPositive();
        assertMatchesFullScan(manifests);
        assertThat(scanner().scan(manifests))
                .singleElement()
                .satisfies(entry -> assertThat(entry.totalBuckets()).isEqualTo(2));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDeletionMatchesCompleteFileIdentity(boolean externalPath) throws Exception {
        writeRows(0);
        ManifestEntry source = table.store().newScan().plan().files().get(0);
        ManifestEntry old =
                ManifestEntry.create(
                        FileKind.ADD,
                        source.partition(),
                        0,
                        4,
                        externalPath
                                ? source.file().newExternalPath("file:/old")
                                : source.file().copy(new byte[] {1}));
        ManifestEntry current =
                ManifestEntry.create(
                        FileKind.ADD,
                        source.partition(),
                        0,
                        2,
                        externalPath
                                ? source.file().newExternalPath("file:/current")
                                : source.file().copy(new byte[] {2}));
        ManifestEntry deleted =
                ManifestEntry.create(FileKind.DELETE, old.partition(), 0, 4, old.file());
        ManifestFile file = table.store().manifestFileFactory().create();
        List<ManifestFileMeta> manifests = file.write(Arrays.asList(old, current, deleted));
        assertThat(scanner().scan(manifests))
                .containsExactly(PartitionEntry.fromManifestEntry(current));
        assertThat(scanner().scan(file.write(Arrays.asList(old, deleted)))).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReusesWholeManifestAndBlockCaches(boolean sidecar) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, sidecar);
        tablePath = new Path(tablePath, "scanner");
        table = createFileStoreTable(true, options, tablePath);
        table.store()
                .setManifestCache(
                        new SegmentsCache<>(1024, MemorySize.ofMebiBytes(16), Long.MAX_VALUE));
        writeRows(0);
        List<ManifestFileMeta> manifests = manifests();
        List<PartitionEntry> expected = scanner().scan(manifests);
        assertThat(expected).hasSize(3);
        for (ManifestFileMeta manifest : manifests) {
            fileIO.delete(new Path(tablePath, "manifest/" + manifest.fileName()), false);
        }
        assertThat(scanner().scan(manifests)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testEmptyScan() {
        assertThat(scanner().scan(Collections.emptyList())).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "whole", "blocks"})
    void testPartitionFilter(String cacheMode) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, cacheMode.equals("blocks"));
        tablePath = new Path(tablePath, "scanner");
        table = createFileStoreTable(true, options, tablePath);
        if (!cacheMode.equals("none")) {
            table.store()
                    .setManifestCache(
                            new SegmentsCache<>(1024, MemorySize.ofMebiBytes(16), Long.MAX_VALUE));
        }
        writeRows(0);
        writeRows(10);
        PartitionPredicate filter =
                PartitionPredicate.fromPredicate(
                        table.schema().logicalPartitionType(),
                        new PredicateBuilder(table.schema().logicalPartitionType()).equal(0, 2));
        PartitionEntryScanner scanner = scanner().withPartitionFilter(filter);
        List<PartitionEntry> expected =
                table.store().newScan().withPartitionFilter(filter).readPartitionEntries();
        assertThat(scanner.scan()).containsExactlyElementsOf(expected);
        assertThat(scanner.scan())
                .singleElement()
                .satisfies(entry -> assertThat(entry.partition().getInt(0)).isEqualTo(2));
        assertThat(scanner.withPartitionFilter(null).scan()).hasSize(3);
    }

    @Test
    void testCustomStrategyRetainsScanContract() throws Exception {
        writeRows(0);
        AtomicInteger calls = new AtomicInteger();
        PartitionUpdateTimeExpireStrategy custom =
                new PartitionUpdateTimeExpireStrategy(
                        table.coreOptions(), table.schema().logicalPartitionType()) {
                    @Override
                    public List<PartitionEntry> selectExpiredPartitions(
                            FileStoreScan scan, LocalDateTime expirationTime) {
                        assertThat(scan.readPartitionEntries()).hasSize(3);
                        calls.incrementAndGet();
                        return Collections.emptyList();
                    }
                };
        assertThat(
                        table.store()
                                .newPartitionExpire(
                                        commitUser,
                                        table,
                                        Duration.ofDays(1),
                                        Duration.ZERO,
                                        custom)
                                .expire(1))
                .isEmpty();
        assertThat(calls).hasValue(1);
    }

    private void writeRows(int start) throws Exception {
        try (BatchTableWrite write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            for (int partition = 1; partition <= 3; partition++) {
                for (int row = start; row < start + 10; row++) {
                    write.write(GenericRow.of(partition, row, (long) row));
                }
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void setCreationTime(List<CommitMessage> messages, LocalDateTime creationTime) {
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        for (CommitMessage message : messages) {
            ((CommitMessageImpl) message)
                    .newFilesIncrement()
                    .newFiles()
                    .replaceAll(
                            file -> {
                                GenericRow row = (GenericRow) serializer.toRow(file);
                                row.setField(
                                        DataFileMeta.SCHEMA.getFieldIndex(
                                                DataFileMeta.CREATION_TIME),
                                        Timestamp.fromLocalDateTime(creationTime));
                                return serializer.fromRow(row);
                            });
        }
    }

    private List<ManifestFileMeta> manifests() {
        return table.store().newScan().manifestsReader().read(null, ScanMode.ALL).filteredManifests;
    }

    private PartitionEntryScanner scanner() {
        return new PartitionEntryScanner(
                table.store().newScan().manifestsReader(), table.store().manifestFileFactory(), 2);
    }

    private void assertMatchesFullScan(List<ManifestFileMeta> manifests) {
        assertThat(scanner().scan(manifests))
                .containsExactlyInAnyOrderElementsOf(
                        table.store().newScan().readPartitionEntries());
    }
}
