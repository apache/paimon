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

package org.apache.paimon.manifest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.ManifestCompactDryRun;
import org.apache.paimon.operation.ManifestFileMerger;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test {@link ManifestFile}. for table without partition */
public class NoPartitionManifestFileMetaTest extends ManifestFileMetaTestBase {
    private final RowType noPartitionType = RowType.of();

    @TempDir java.nio.file.Path tempDir;
    private ManifestFile manifestFile;

    @BeforeEach
    public void beforeEach() {
        manifestFile = createManifestFile(tempDir.toString());
    }

    @Test
    public void testMerge() {
        List<ManifestFileMeta> input = createBaseManifestFileMetas(false);
        addDeltaManifests(input, false);

        Options testOptions = new Options();
        testOptions.set("manifest.target-file-size", "500B");
        testOptions.set("manifest.merge-min-count", "3");
        testOptions.set("manifest.full-compaction-threshold-size", "200B");
        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));
        assertEquivalentEntries(input, merged);

        // the first one is not deleted, it should not be merged
        assertThat(merged.get(0)).isSameAs(input.get(0));
    }

    @Test
    public void testMergeFullCompactionWithoutDeleteFile() {
        // entries are All ADD.
        List<ManifestFileMeta> input = new ArrayList<>();
        // base
        for (int j = 0; j < 6; j++) {
            List<ManifestEntry> entrys = new ArrayList<>();
            for (int i = 1; i < 50; i++) {
                entrys.add(makeEntry(true, String.format(manifestFileNameTemplate, j, i), null));
            }
            input.add(makeManifest(entrys.toArray(new ManifestEntry[0])));
        }
        // The base file all meet the manifest file size.
        long threshold = input.stream().mapToLong(ManifestFileMeta::fileSize).min().getAsLong();
        Set<String> baseFiles =
                input.stream().map(ManifestFileMeta::fileName).collect(Collectors.toSet());

        // assert base manifest are not accessed
        for (String baseFile : baseFiles) {
            manifestFile.delete(baseFile);
        }

        // delta
        input.add(makeManifest(makeEntry(true, "A", null)));
        input.add(makeManifest(makeEntry(true, "B", null)));
        input.add(makeManifest(makeEntry(true, "C", null)));
        input.add(makeManifest(makeEntry(true, "D", null)));
        input.add(makeManifest(makeEntry(true, "E", null)));
        input.add(makeManifest(makeEntry(true, "F", null)));
        input.add(makeManifest(makeEntry(true, "G", null)));

        Options testOptions = new Options();
        testOptions.set("manifest.target-file-size", threshold + "B");
        testOptions.set("manifest.merge-min-count", "3");
        testOptions.set("manifest.full-compaction-threshold-size", "200B");
        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));
        assertEquivalentEntries(
                input.stream()
                        .filter(f -> !baseFiles.contains(f.fileName()))
                        .collect(Collectors.toList()),
                merged.stream()
                        .filter(f -> !baseFiles.contains(f.fileName()))
                        .collect(Collectors.toList()));
    }

    @Test
    public void testDataEvolutionManifestSortByRowId() {
        List<ManifestFileMeta> input = new ArrayList<>();
        input.add(makeManifest(makeRowIdEntry("row20", 20, 5, 0), makeRowIdEntry("row0", 0, 5, 0)));
        input.add(
                makeManifest(
                        makeRowIdEntry("row10-seq1", 10, 5, 1),
                        makeRowIdEntry("row10-seq3", 10, 5, 3)));

        Options testOptions = new Options();
        testOptions.set("manifest-sort.enabled", "true");
        testOptions.set("row-tracking.enabled", "true");
        testOptions.set("data-evolution.enabled", "true");

        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));

        assertEquivalentEntries(input, merged);

        List<String> outputFileNames = new ArrayList<>();
        for (ManifestFileMeta meta : merged) {
            for (ManifestEntry entry : manifestFile.read(meta.fileName(), meta.fileSize())) {
                outputFileNames.add(entry.file().fileName());
            }
        }
        assertThat(outputFileNames).containsExactly("row0", "row10-seq3", "row10-seq1", "row20");
    }

    @ParameterizedTest
    @CsvSource({
        "4, false, false", "4, true, false", "-2, false, false", "-2, true, false",
        "4, false, true", "4, true, true", "-2, false, true", "-2, true, true"
    })
    public void testManifestSortByBucket(
            int bucket, boolean fullCompaction, boolean missingBucketStats) {
        int firstBucket = bucket == -2 ? -2 : 0;
        List<ManifestFileMeta> input =
                Arrays.asList(
                        makeManifest(
                                makeBucketEntry(true, "a-high", 3),
                                makeBucketEntry(true, "same-name", 1),
                                makeBucketEntry(true, "z-low", firstBucket)),
                        makeManifest(
                                makeBucketEntry(true, "same-name", 2),
                                makeBucketEntry(false, "same-name", 1),
                                makeBucketEntry(true, "b-high", 3)));
        if (missingBucketStats) {
            input.set(0, withoutBucketStats(input.get(0)));
        }

        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SORT_ENABLED, true);
        options.set(CoreOptions.BUCKET, bucket);
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), "1G");
        options.set(CoreOptions.MANIFEST_MERGE_MIN_COUNT, 100);
        options.set(
                CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(),
                fullCompaction ? "1B" : Long.MAX_VALUE + "B");

        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input, manifestFile, getPartitionType(), new CoreOptions(options));
        List<ManifestEntry> entries =
                merged.stream()
                        .flatMap(
                                meta ->
                                        manifestFile.read(meta.fileName(), meta.fileSize())
                                                .stream())
                        .collect(Collectors.toList());

        assertThat(entries)
                .containsExactly(
                        makeBucketEntry(true, "z-low", firstBucket),
                        makeBucketEntry(true, "same-name", 2),
                        makeBucketEntry(true, "a-high", 3),
                        makeBucketEntry(true, "b-high", 3));
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).minBucket()).isEqualTo(firstBucket);
        assertThat(merged.get(0).maxBucket()).isEqualTo(3);
    }

    @ParameterizedTest
    @CsvSource({
        "4, 0, false", "4, 1, false", "4, 2, false",
        "-2, 0, false", "-2, 1, false", "-2, 2, false",
        "4, 0, true", "-2, 0, true"
    })
    public void testManifestSortDryRunByBucket(
            int bucket, int lowMaxBucket, boolean missingBucketStats) {
        int firstBucket = bucket == -2 ? -2 : 0;
        List<ManifestFileMeta> input =
                Arrays.asList(
                        makeManifest(
                                makeBucketEntry(true, "high-start", 1),
                                makeBucketEntry(true, "high-end", 3)),
                        makeManifest(
                                makeBucketEntry(true, "low-start", firstBucket),
                                makeBucketEntry(true, "low-end", lowMaxBucket)));
        if (missingBucketStats) {
            input.set(0, withoutBucketStats(input.get(0)));
        }

        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SORT_ENABLED, true);
        options.set(CoreOptions.BUCKET, bucket);
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), "1B");
        options.set(CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), Long.MAX_VALUE + "B");

        FileStoreTable table = mock(FileStoreTable.class, RETURNS_DEEP_STUBS);
        Snapshot snapshot = mock(Snapshot.class);
        when(table.options()).thenReturn(options.toMap());
        when(table.store().snapshotManager().latestSnapshot()).thenReturn(snapshot);
        when(table.store().manifestListFactory().create().readDataManifests(snapshot))
                .thenReturn(input);
        when(table.store().manifestFileFactory().create()).thenReturn(manifestFile);
        when(table.schema().logicalPartitionType()).thenReturn(getPartitionType());

        assertThat(ManifestCompactDryRun.execute(table))
                .endsWith(
                        missingBucketStats || lowMaxBucket > 1
                                ? "Manifest sort level files: L0=0, L1=0, L2=0, L3=1, L4=1."
                                : "Manifest sort level files: L0=0, L1=0, L2=0, L3=0, L4=2.");
    }

    private ManifestEntry makeBucketEntry(boolean isAdd, String fileName, int bucket) {
        ManifestEntry entry = makeEntry(isAdd, fileName, null);
        return ManifestEntry.create(entry.kind(), entry.partition(), bucket, 4, entry.file());
    }

    private ManifestFileMeta withoutBucketStats(ManifestFileMeta meta) {
        return new ManifestFileMeta(
                meta.fileName(),
                meta.fileSize(),
                meta.numAddedFiles(),
                meta.numDeletedFiles(),
                meta.partitionStats(),
                meta.schemaId(),
                null,
                null,
                meta.minLevel(),
                meta.maxLevel(),
                meta.minRowId(),
                meta.maxRowId(),
                meta.totalBuckets(),
                meta.extraFiles());
    }

    @Override
    public ManifestFile getManifestFile() {
        return manifestFile;
    }

    @Override
    public RowType getPartitionType() {
        return noPartitionType;
    }

    private ManifestEntry makeRowIdEntry(
            String fileName, long firstRowId, long rowCount, long sequenceNumber) {
        return ManifestEntry.create(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                0,
                DataFileMeta.create(
                        fileName,
                        0,
                        rowCount,
                        BinaryRow.EMPTY_ROW,
                        BinaryRow.EMPTY_ROW,
                        StatsTestUtils.newEmptySimpleStats(),
                        StatsTestUtils.newEmptySimpleStats(),
                        sequenceNumber,
                        sequenceNumber,
                        0,
                        0,
                        Collections.emptyList(),
                        Timestamp.fromEpochMillis(200000),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        firstRowId,
                        Collections.singletonList("f0"),
                        null));
    }
}
