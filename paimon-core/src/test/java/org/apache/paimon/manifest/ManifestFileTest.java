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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.paimon.TestKeyValueGenerator.DEFAULT_PART_TYPE;
import static org.apache.paimon.stats.StatsTestUtils.convertWithoutSchemaEvolution;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ManifestFile}. */
public class ManifestFileTest {

    private final ManifestTestDataGenerator gen = ManifestTestDataGenerator.builder().build();
    private final FileFormat avro = FileFormat.fromIdentifier("avro", new Options());

    @TempDir java.nio.file.Path tempDir;

    @RepeatedTest(10)
    public void testWriteAndReadManifestFile() {
        List<ManifestEntry> entries = generateData();
        ManifestFileMeta meta = gen.createManifestFileMeta(entries);
        System.out.println(tempDir.toString());
        ManifestFile manifestFile = createManifestFile(tempDir.toString());

        List<ManifestFileMeta> actualMetas = manifestFile.write(entries);
        checkRollingFiles(meta, actualMetas, manifestFile.suggestedFileSize());
        List<ManifestEntry> actualEntries =
                actualMetas.stream()
                        .flatMap(m -> manifestFile.read(m.fileName(), m.fileSize()).stream())
                        .collect(Collectors.toList());
        assertThat(actualEntries).isEqualTo(entries);
    }

    @RepeatedTest(10)
    public void testCleanUpForException() throws IOException {
        String failingName = UUID.randomUUID().toString();
        FailingFileIO.reset(failingName, 1, 10);
        List<ManifestEntry> entries = generateData();
        ManifestFile manifestFile =
                createManifestFile(FailingFileIO.getFailingPath(failingName, tempDir.toString()));

        try {
            manifestFile.write(entries);
        } catch (Throwable e) {
            assertThat(e).hasRootCauseExactlyInstanceOf(FailingFileIO.ArtificialException.class);
            Path manifestDir = new Path(tempDir.toString() + "/manifest");
            assertThat(LocalFileIO.create().listStatus(manifestDir)).isEmpty();
        }
    }

    @Test
    void testManifestCreationTimeTimestamp() {
        List<ManifestEntry> entries = generateData();
        ManifestFile manifestFile = createManifestFile(tempDir.toString());

        List<ManifestFileMeta> actualMetas = manifestFile.write(entries);
        List<ManifestEntry> actualEntries =
                actualMetas.stream()
                        .flatMap(m -> manifestFile.read(m.fileName(), m.fileSize()).stream())
                        .collect(Collectors.toList());

        int creationTimesFound = 0;
        for (ManifestEntry entry : actualEntries) {
            if (entry.file().creationTime() != null) {
                creationTimesFound++;
                org.apache.paimon.data.Timestamp creationTime = entry.file().creationTime();
                assertThat(creationTime).isNotNull();
                long epochMillis = entry.file().creationTimeEpochMillis();
                assertThat(epochMillis).isPositive();
                long expectedEpochMillis = creationTime.getMillisecond();
                java.time.ZoneId systemZone = java.time.ZoneId.systemDefault();
                java.time.ZoneOffset offset =
                        systemZone
                                .getRules()
                                .getOffset(java.time.Instant.ofEpochMilli(expectedEpochMillis));
                expectedEpochMillis = expectedEpochMillis - (offset.getTotalSeconds() * 1000L);
                assertThat(epochMillis).isEqualTo(expectedEpochMillis);
            }
        }

        assertThat(creationTimesFound).isPositive();
    }

    @Test
    void testReadDeletedEntriesWithProjectedScan() {
        ManifestEntry first = gen.next();
        ManifestEntry second = gen.next();
        DataFileMeta firstFile =
                first.file()
                        .copy(Arrays.asList("extra-1", "extra-2"))
                        .copy(new byte[] {1, 2})
                        .newExternalPath("external/first")
                        .newFirstRowId(10L);
        DataFileMeta secondFile =
                second.file()
                        .copy(Arrays.asList("extra-3"))
                        .copy(new byte[] {3, 4})
                        .newExternalPath("external/second")
                        .newFirstRowId(20L);
        ManifestEntry firstAdd =
                ManifestEntry.create(
                        FileKind.ADD,
                        first.partition(),
                        first.bucket(),
                        first.totalBuckets(),
                        firstFile);
        ManifestEntry firstDelete =
                ManifestEntry.create(
                        FileKind.DELETE,
                        first.partition(),
                        first.bucket(),
                        first.totalBuckets(),
                        firstFile);
        ManifestEntry secondAdd =
                ManifestEntry.create(
                        FileKind.ADD,
                        second.partition(),
                        second.bucket(),
                        second.totalBuckets(),
                        secondFile);
        ManifestEntry secondDelete =
                ManifestEntry.create(
                        FileKind.DELETE,
                        second.partition(),
                        second.bucket(),
                        second.totalBuckets(),
                        secondFile);
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta firstManifest =
                writeSingleManifest(manifestFile, Arrays.asList(firstAdd, firstDelete));
        ManifestFileMeta secondManifest =
                writeSingleManifest(manifestFile, Arrays.asList(secondAdd, secondDelete));

        Set<FileEntry.Identifier> deleted =
                FileEntry.readDeletedEntries(
                        manifestFile, Arrays.asList(firstManifest, secondManifest), 2);

        assertThat(deleted)
                .containsExactlyInAnyOrder(firstDelete.identifier(), secondDelete.identifier());
    }

    @Test
    void testReadExpireFileEntriesWithProjectedScan() {
        ManifestEntry source = gen.next();
        DataFileMeta file =
                source.file()
                        .copy(Arrays.asList("extra-1", "extra-2"))
                        .copy(new byte[] {1, 2})
                        .newExternalPath("external/data-file")
                        .newFirstRowId(10L);
        List<ManifestEntry> entries =
                Arrays.asList(
                        ManifestEntry.create(
                                FileKind.ADD,
                                source.partition(),
                                source.bucket(),
                                source.totalBuckets(),
                                file),
                        ManifestEntry.create(
                                FileKind.DELETE,
                                source.partition(),
                                source.bucket(),
                                source.totalBuckets(),
                                file));
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);

        List<ExpireFileEntry> actual =
                manifestFile.readExpireFileEntries(manifest.fileName(), manifest.fileSize());
        List<ExpireFileEntry> expected =
                entries.stream().map(ExpireFileEntry::from).collect(Collectors.toList());

        assertThat(actual).containsExactlyElementsOf(expected);
        for (int i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i).embeddedIndex())
                    .containsExactly(expected.get(i).embeddedIndex());
            assertThat(actual.get(i).fileSource()).isEqualTo(expected.get(i).fileSource());
        }
    }

    @Test
    void testScanProjectedManifestEntries() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        BinaryManifestEntry.Projection projection =
                projection(DataFileMeta.FILE_NAME, DataFileMeta.ROW_COUNT);
        List<String> fileNames = new ArrayList<>();
        List<Long> rowCounts = new ArrayList<>();
        AtomicReference<BinaryManifestEntry> retained = new AtomicReference<>();

        try (CloseableIterator<BinaryManifestEntry> iterator =
                manifestFile.scan(manifest.fileName(), manifest.fileSize(), projection)) {
            while (iterator.hasNext()) {
                BinaryManifestEntry entry = iterator.next();
                BinaryManifestEntry previous = retained.getAndSet(entry);
                if (previous != null) {
                    assertThat(entry).isSameAs(previous);
                }
                fileNames.add(entry.fileName());
                rowCounts.add(entry.rowCount());
            }
        }

        assertThat(fileNames)
                .containsExactlyElementsOf(
                        entries.stream().map(ManifestEntry::fileName).collect(Collectors.toList()));
        assertThat(rowCounts)
                .containsExactlyElementsOf(
                        entries.stream().map(ManifestEntry::rowCount).collect(Collectors.toList()));
        assertCleared(retained.get());
    }

    @Test
    void testScanProjectedManifestInvalidatesEntryWhenAdvancing() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);

        try (CloseableIterator<BinaryManifestEntry> iterator =
                manifestFile.scan(
                        manifest.fileName(),
                        manifest.fileSize(),
                        projection(DataFileMeta.FILE_NAME))) {
            assertThat(iterator.hasNext()).isTrue();
            BinaryManifestEntry first = iterator.next();
            assertThat(first.fileName()).isEqualTo(entries.get(0).fileName());

            assertThat(iterator.hasNext()).isTrue();
            assertCleared(first);
            BinaryManifestEntry second = iterator.next();
            assertThat(second).isSameAs(first);
            assertThat(second.fileName()).isEqualTo(entries.get(1).fileName());
        }
    }

    @Test
    void testScanProjectedManifestCanStopEarly() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        AtomicInteger visited = new AtomicInteger();
        AtomicReference<BinaryManifestEntry> retained = new AtomicReference<>();

        try (CloseableIterator<BinaryManifestEntry> iterator =
                manifestFile.scan(
                        manifest.fileName(),
                        manifest.fileSize(),
                        projection(DataFileMeta.FILE_NAME))) {
            while (iterator.hasNext()) {
                BinaryManifestEntry entry = iterator.next();
                retained.set(entry);
                visited.incrementAndGet();
                break;
            }
        }

        assertThat(visited).hasValue(1);
        assertCleared(retained.get());
    }

    @Test
    void testScanProjectedManifestClearsEntryWhenProcessingFails() {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        RuntimeException failure = new RuntimeException("Expected processing failure.");
        AtomicReference<BinaryManifestEntry> retained = new AtomicReference<>();

        assertThatThrownBy(
                        () -> {
                            try (CloseableIterator<BinaryManifestEntry> iterator =
                                    manifestFile.scan(
                                            manifest.fileName(),
                                            manifest.fileSize(),
                                            projection(DataFileMeta.FILE_NAME))) {
                                assertThat(iterator.hasNext()).isTrue();
                                BinaryManifestEntry entry = iterator.next();
                                retained.set(entry);
                                throw failure;
                            }
                        })
                .isSameAs(failure);
        assertCleared(retained.get());
    }

    private List<ManifestEntry> generateData() {
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            entries.add(gen.next());
        }
        return entries;
    }

    private ManifestFile createManifestFile(String pathStr) {
        return createManifestFile(pathStr, ThreadLocalRandom.current().nextInt(8192) + 1024);
    }

    private ManifestFile createManifestFile(String pathStr, long suggestedFileSize) {
        Path path = new Path(pathStr);
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        path,
                        DEFAULT_PART_TYPE,
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.PARTITION_GENERATE_LEGACY_NAME.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null,
                        null,
                        CoreOptions.ExternalPathStrategy.NONE,
                        null,
                        false,
                        null);
        FileIO fileIO = FileIOFinder.find(path);
        return new ManifestFile.Factory(
                        fileIO,
                        new SchemaManager(fileIO, path),
                        DEFAULT_PART_TYPE,
                        avro,
                        "zstd",
                        pathFactory,
                        suggestedFileSize,
                        null)
                .create();
    }

    private ManifestFileMeta writeSingleManifest(
            ManifestFile manifestFile, List<ManifestEntry> entries) {
        List<ManifestFileMeta> manifests = manifestFile.write(entries);
        assertThat(manifests).hasSize(1);
        return manifests.get(0);
    }

    private BinaryManifestEntry.Projection projection(String... projectedFileFields) {
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        List<DataField> fields =
                Arrays.asList(
                        manifestType.getField(ManifestEntry.KIND),
                        manifestType.getField(ManifestEntry.PARTITION),
                        manifestType
                                .getField(ManifestEntry.FILE)
                                .newType(DataFileMeta.SCHEMA.project(projectedFileFields)));
        return BinaryManifestEntry.Projection.create(new RowType(false, fields));
    }

    private static void assertCleared(BinaryManifestEntry entry) {
        assertThatThrownBy(entry::fileName)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not backed by a row");
    }

    private void checkRollingFiles(
            ManifestFileMeta expected, List<ManifestFileMeta> actual, long suggestedFileSize) {
        // all but last file should be no smaller than suggestedFileSize
        for (int i = 0; i + 1 < actual.size(); i++) {
            assertThat(actual.get(i).fileSize() >= suggestedFileSize).isTrue();
        }

        // expected.numAddedFiles == sum(numAddedFiles)
        assertThat(actual.stream().mapToLong(ManifestFileMeta::numAddedFiles).sum())
                .isEqualTo(expected.numAddedFiles());

        // expected.numDeletedFiles == sum(numDeletedFiles)
        assertThat(actual.stream().mapToLong(ManifestFileMeta::numDeletedFiles).sum())
                .isEqualTo(expected.numDeletedFiles());

        // check stats
        SimpleColStats[] fieldStats =
                convertWithoutSchemaEvolution(expected.partitionStats(), DEFAULT_PART_TYPE);
        for (int i = 0; i < fieldStats.length; i++) {
            int idx = i;
            StatsTestUtils.checkRollingFileStats(
                    fieldStats[i],
                    actual,
                    meta ->
                            convertWithoutSchemaEvolution(meta.partitionStats(), DEFAULT_PART_TYPE)[
                                    idx]);
        }
    }
}
