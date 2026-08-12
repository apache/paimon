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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.FileSystemSchemaManager;
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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
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

    @Test
    void testReadMissingManifestFile() {
        ManifestFile manifestFile = createManifestFile(tempDir.toString());

        assertThatThrownBy(
                        () ->
                                manifestFile.read(
                                        "missing-manifest",
                                        null,
                                        null,
                                        null,
                                        row -> true,
                                        entry -> true))
                .hasMessageContaining("not found");
    }

    @Test
    void testAvroReaderSkipsDataFileMetaBeforeMaterialization() throws Exception {
        List<ManifestEntry> entries = generateData();
        ManifestEntry selected = entries.get(0);
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromMultiple(
                        DEFAULT_PART_TYPE, Collections.singletonList(selected.partition()));
        BucketFilter bucketFilter = new BucketFilter(false, selected.bucket(), null, null);
        List<ManifestEntry> expected =
                entries.stream()
                        .filter(
                                entry ->
                                        entry.partition().equals(selected.partition())
                                                && entry.bucket() == selected.bucket())
                        .collect(Collectors.toList());

        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        FileIO fileIO = LocalFileIO.create();
        Path path = new Path(new Path(tempDir.toUri()), "manifest/" + manifest.fileName());
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();
        List<ManifestEntry> actual = new ArrayList<>();

        try (ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path));
                CloseableIterator<InternalRow> rows =
                        reader.read(
                                ManifestEntry.MANIFEST_ROW_TYPE, partitionFilter, bucketFilter)) {
            while (rows.hasNext()) {
                InternalRow row = rows.next();
                actual.add(serializer.fromRow(row));
            }
        }

        assertThat(actual).containsExactlyElementsOf(expected);
        assertThat(
                        manifestFile.read(
                                manifest.fileName(),
                                manifest.fileSize(),
                                partitionFilter,
                                bucketFilter,
                                row -> true,
                                entry -> true))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void testAvroReaderSupportsReorderedNestedProjection() throws Exception {
        List<ManifestEntry> entries = generateData();
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        Path path = new Path(new Path(tempDir.toUri()), "manifest/" + manifest.fileName());
        LocalFileIO fileIO = LocalFileIO.create();

        List<DataField> fields = ManifestEntry.MANIFEST_ROW_TYPE.getFields();
        RowType projectedFileType =
                DataFileMeta.SCHEMA.project(DataFileMeta.FILE_NAME, DataFileMeta.ROW_COUNT);
        RowType projectedType =
                new RowType(
                        false,
                        Arrays.asList(
                                fields.get(5).newType(projectedFileType),
                                fields.get(2),
                                fields.get(1)));
        ProjectedManifestEntry projectedEntry =
                ProjectedManifestEntry.Projection.create(projectedType).createEntry();

        try (ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path));
                CloseableIterator<InternalRow> rows = reader.read(projectedType, null, null)) {
            for (ManifestEntry expected : entries) {
                assertThat(rows.hasNext()).isTrue();
                InternalRow row = rows.next();
                assertThat(row.getFieldCount()).isEqualTo(3);
                assertThat(row.getRow(0, projectedFileType.getFieldCount()).getFieldCount())
                        .isEqualTo(2);

                projectedEntry.replace(row);
                assertThat(projectedEntry.fileName()).isEqualTo(expected.fileName());
                assertThat(projectedEntry.rowCount()).isEqualTo(expected.rowCount());
                assertThat(projectedEntry.partition()).isEqualTo(expected.partition());
                assertThat(projectedEntry.kind()).isEqualTo(expected.kind());
            }
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testAvroReaderSkipsUnprojectedDataFile() throws Exception {
        List<ManifestEntry> entries = generateData();
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        Path path = new Path(new Path(tempDir.toUri()), "manifest/" + manifest.fileName());
        LocalFileIO fileIO = LocalFileIO.create();

        List<DataField> fields = ManifestEntry.MANIFEST_ROW_TYPE.getFields();
        RowType projectedType = new RowType(false, Arrays.asList(fields.get(2), fields.get(1)));
        try (ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path));
                CloseableIterator<InternalRow> rows = reader.read(projectedType, null, null)) {
            for (ManifestEntry expected : entries) {
                assertThat(rows.hasNext()).isTrue();
                InternalRow row = rows.next();
                assertThat(row.getFieldCount()).isEqualTo(2);
                assertThat(row.getBinary(0))
                        .containsExactly(
                                org.apache.paimon.utils.SerializationUtils.serializeBinaryRow(
                                        expected.partition()));
                assertThat(FileKind.fromByteValue(row.getByte(1))).isEqualTo(expected.kind());
            }
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testAvroReaderRejectsTrailingUndecodedRecords() throws Exception {
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest =
                writeSingleManifest(manifestFile, Arrays.asList(gen.next(), gen.next()));
        Path path = new Path(new Path(tempDir.toUri()), "manifest/" + manifest.fileName());
        lowerFirstBlockRecordCount(path);

        try (ManifestAvroReader reader =
                        new ManifestAvroReader(LocalFileIO.create().newInputStream(path));
                CloseableIterator<InternalRow> rows =
                        reader.read(ManifestEntry.MANIFEST_ROW_TYPE, null, null)) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThatThrownBy(rows::hasNext)
                    .isInstanceOf(UncheckedIOException.class)
                    .hasRootCauseInstanceOf(IOException.class)
                    .hasStackTraceContaining("trailing undecoded bytes");
        }
    }

    @Test
    void testProjectedScanRejectsUnsupportedFormatIdentifier() throws Exception {
        ManifestEntry entry = gen.next();
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest =
                writeSingleManifest(manifestFile, Collections.singletonList(entry));
        Path path = new Path(new Path(tempDir.toUri()), "manifest/" + manifest.fileName());
        LocalFileIO fileIO = LocalFileIO.create();
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();
        InternalRow valid = serializer.toRow(entry);

        try (PositionOutputStream out = fileIO.newOutputStream(path, true);
                FormatWriter writer =
                        avro.createWriterFactory(ManifestEntry.MANIFEST_ROW_TYPE)
                                .create(out, "zstd")) {
            writer.addElement(
                    GenericRow.of(
                            1,
                            valid.getByte(1),
                            valid.getBinary(2),
                            valid.getInt(3),
                            valid.getInt(4),
                            valid.getRow(5, DataFileMeta.SCHEMA.getFieldCount())));
        }

        try (CloseableIterator<ProjectedManifestEntry> entries =
                manifestFile.scan(
                        manifest.fileName(), ProjectedManifestEntry.DELETE_ENTRY_PROJECTION)) {
            assertThat(entries.hasNext()).isTrue();
            assertThatThrownBy(entries::next)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not compatible");
        }
    }

    @Test
    void testAvroReaderReadsLegacyDataFileMetaWithFewerFields() throws Exception {
        ManifestEntry generated = gen.next();
        DataFileMeta sourceFile = generated.file().newFirstRowId(42L);
        ManifestEntry source =
                ManifestEntry.create(
                        FileKind.ADD,
                        generated.partition(),
                        generated.bucket(),
                        generated.totalBuckets(),
                        sourceFile);
        RowType legacyFileType =
                DataFileMeta.SCHEMA.project(
                        new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17});
        List<DataField> legacyManifestFields =
                ManifestEntry.MANIFEST_ROW_TYPE.getFields().stream()
                        .map(
                                field ->
                                        ManifestEntry.FILE.equals(field.name())
                                                ? field.newType(legacyFileType)
                                                : field)
                        .collect(Collectors.toList());
        RowType legacyManifestType = new RowType(false, legacyManifestFields);
        Path path = new Path(new Path(tempDir.toUri()), "legacy-manifest.avro");
        LocalFileIO fileIO = LocalFileIO.create();
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();

        try (PositionOutputStream out = fileIO.newOutputStream(path, false);
                FormatWriter writer =
                        avro.createWriterFactory(legacyManifestType).create(out, "zstd")) {
            writer.addElement(serializer.toRow(source));
        }

        ManifestEntry actual;
        try (ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path));
                CloseableIterator<InternalRow> rows =
                        reader.read(ManifestEntry.MANIFEST_ROW_TYPE, null, null)) {
            assertThat(rows.hasNext()).isTrue();
            actual = serializer.fromRow(rows.next());
            assertThat(rows.hasNext()).isFalse();
        }

        assertThat(actual.fileName()).isEqualTo(source.fileName());
        assertThat(actual.file().firstRowId()).isNull();
        assertThat(actual.file().writeCols()).isNull();

        ProjectedManifestEntry.Projection projection = ProjectedManifestEntry.ROW_RANGE_PROJECTION;
        ProjectedManifestEntry binaryEntry = projection.createEntry();
        try (ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path))) {
            assertThat(reader.hasNext()).isTrue();
            ManifestAvroReader.RawBlock block = reader.next();
            assertThat(block.rawBlockCopySupported()).isFalse();
            ManifestAvroReader.RowIterator rows = block.toRows(projection.projectedType());
            assertThat(rows.hasNext()).isTrue();
            binaryEntry.replace(rows.next());
            assertThat(binaryEntry.rowCount()).isEqualTo(source.rowCount());
            assertThat(binaryEntry.firstRowId()).isNull();
            assertThat(rows.hasNext()).isFalse();
            assertThat(reader.hasNext()).isFalse();
        }
    }

    @Test
    void testAvroReaderRejectsReorderedTopLevelFields() throws Exception {
        ManifestEntry entry = gen.next();
        List<DataField> fields = ManifestEntry.MANIFEST_ROW_TYPE.getFields();
        RowType reorderedType =
                new RowType(
                        false,
                        Arrays.asList(
                                fields.get(0),
                                fields.get(5),
                                fields.get(1),
                                fields.get(2),
                                fields.get(3),
                                fields.get(4)));
        Path path = new Path(new Path(tempDir.toUri()), "reordered-manifest.avro");
        LocalFileIO fileIO = LocalFileIO.create();
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();

        try (PositionOutputStream out = fileIO.newOutputStream(path, false);
                FormatWriter writer = avro.createWriterFactory(reorderedType).create(out, "zstd")) {
            InternalRow row = serializer.toRow(entry);
            writer.addElement(
                    GenericRow.of(
                            row.getInt(0),
                            row.getRow(5, DataFileMeta.SCHEMA.getFieldCount()),
                            row.getByte(1),
                            row.getBinary(2),
                            row.getInt(3),
                            row.getInt(4)));
        }

        assertThatThrownBy(
                        () -> {
                            try (ManifestAvroReader reader =
                                            new ManifestAvroReader(fileIO.newInputStream(path));
                                    CloseableIterator<InternalRow> rows =
                                            reader.read(
                                                    ManifestEntry.MANIFEST_ROW_TYPE, null, null)) {
                                rows.hasNext();
                            }
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expected _KIND but found _FILE");
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
    void testReadDeletedEntriesWithProjectedScan() throws Exception {
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

        try (CloseableIterator<ProjectedManifestEntry> entries =
                manifestFile.scan(
                        firstManifest.fileName(), ProjectedManifestEntry.DELETE_ENTRY_PROJECTION)) {
            assertThat(entries.next().file().nonNullFirstRowId()).isEqualTo(10L);
            assertThat(entries.next().file().nonNullFirstRowId()).isEqualTo(10L);
            assertThat(entries.hasNext()).isFalse();
        }

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

        List<ExpireFileEntry> actual = manifestFile.readExpireFileEntries(manifest.fileName());
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
    void testScanProjectedManifestCreatesDistinctEntryWrappers() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        ProjectedManifestEntry.Projection projection =
                projection(DataFileMeta.FILE_NAME, DataFileMeta.ROW_COUNT);
        List<String> fileNames = new ArrayList<>();
        List<Long> rowCounts = new ArrayList<>();
        ProjectedManifestEntry previous = null;

        try (CloseableIterator<ProjectedManifestEntry> iterator =
                manifestFile.scan(manifest.fileName(), projection)) {
            while (iterator.hasNext()) {
                ProjectedManifestEntry current = iterator.next();
                assertThat(current).isNotSameAs(previous);
                fileNames.add(current.fileName());
                rowCounts.add(current.rowCount());
                previous = current;
            }
        }

        assertThat(fileNames)
                .containsExactlyElementsOf(
                        entries.stream().map(ManifestEntry::fileName).collect(Collectors.toList()));
        assertThat(rowCounts)
                .containsExactlyElementsOf(
                        entries.stream().map(ManifestEntry::rowCount).collect(Collectors.toList()));
    }

    @Test
    void testBlockReaderConvertsRawBlocksToProjectedRows() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        ProjectedManifestEntry.Projection projection =
                projection(DataFileMeta.FILE_NAME, DataFileMeta.ROW_COUNT);
        ProjectedManifestEntry actual = projection.createEntry();
        InternalRow reusedRow = null;
        InternalRow reusedFileRow = null;
        int position = 0;

        try (ManifestAvroReader reader = openManifestReader(manifest)) {
            while (reader.hasNext()) {
                ManifestAvroReader.RawBlock block = reader.next();
                assertThat(block.rawBlockCopySupported()).isTrue();
                ManifestAvroReader.RowIterator rows = block.toRows(projection.projectedType());
                ByteBuffer reusedEncodedRecord = null;
                while (rows.hasNext()) {
                    GenericRow row = rows.next();
                    ByteBuffer encodedRecord = rows.encodedRecord();
                    assertThat(encodedRecord.remaining()).isPositive();
                    if (reusedEncodedRecord != null) {
                        assertThat(encodedRecord).isSameAs(reusedEncodedRecord);
                    }
                    reusedEncodedRecord = encodedRecord;
                    InternalRow fileRow = row.getRow(2, 2);
                    if (reusedRow != null) {
                        assertThat(row).isSameAs(reusedRow);
                        assertThat(fileRow).isSameAs(reusedFileRow);
                    }
                    reusedRow = row;
                    reusedFileRow = fileRow;
                    actual.replace(row);
                    ManifestEntry expected = entries.get(position++);
                    assertThat(actual.kind()).isEqualTo(expected.kind());
                    assertThat(actual.partition()).isEqualTo(expected.partition());
                    assertThat(actual.fileName()).isEqualTo(expected.fileName());
                    assertThat(actual.rowCount()).isEqualTo(expected.rowCount());
                }
            }
        }

        assertThat(position).isEqualTo(entries.size());
    }

    @Test
    void testBlockReaderReadsAcrossMultipleBlocks() throws Exception {
        List<ManifestEntry> entries = Collections.nCopies(1_000, gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        ProjectedManifestEntry.Projection projection = projection(DataFileMeta.FILE_NAME);
        int blockCount = 0;
        int rowCount = 0;

        try (ManifestAvroReader reader = openManifestReader(manifest)) {
            while (reader.hasNext()) {
                ManifestAvroReader.RowIterator rows =
                        reader.next().toRows(projection.projectedType());
                assertThat(rows.hasNext()).isTrue();
                while (rows.hasNext()) {
                    rows.next();
                    rowCount++;
                }
                blockCount++;
            }
        }

        assertThat(blockCount).isGreaterThan(1);
        assertThat(rowCount).isEqualTo(entries.size());
    }

    @Test
    void testBlockReaderSupportsReorderedProjection() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        RowType fileType =
                DataFileMeta.SCHEMA.project(DataFileMeta.ROW_COUNT, DataFileMeta.FILE_NAME);
        RowType projectedType =
                new RowType(
                        false,
                        Arrays.asList(
                                manifestType.getField(ManifestEntry.FILE).newType(fileType),
                                manifestType.getField(ManifestEntry.PARTITION),
                                manifestType.getField(ManifestEntry.KIND)));
        ProjectedManifestEntry.Projection projection =
                ProjectedManifestEntry.Projection.create(projectedType);
        ProjectedManifestEntry actual = projection.createEntry();
        int position = 0;

        try (ManifestAvroReader reader = openManifestReader(manifest)) {
            while (reader.hasNext()) {
                ManifestAvroReader.RowIterator rows =
                        reader.next().toRows(projection.projectedType());
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    assertThat(row.getRow(0, 2).getLong(0))
                            .isEqualTo(entries.get(position).rowCount());
                    actual.replace(row);
                    assertThat(actual.fileName()).isEqualTo(entries.get(position).fileName());
                    assertThat(actual.partition()).isEqualTo(entries.get(position).partition());
                    assertThat(actual.kind()).isEqualTo(entries.get(position).kind());
                    position++;
                }
            }
        }

        assertThat(position).isEqualTo(entries.size());
    }

    @Test
    void testBlockReaderSupportsFullManifestProjection() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        ProjectedManifestEntry.Projection projection = ProjectedManifestEntry.fullProjection();
        ProjectedManifestEntry binaryEntry = projection.createEntry();
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();
        int position = 0;

        try (ManifestAvroReader reader = openManifestReader(manifest)) {
            while (reader.hasNext()) {
                ManifestAvroReader.RowIterator rows =
                        reader.next().toRows(projection.projectedType());
                while (rows.hasNext()) {
                    binaryEntry.replace(rows.next());
                    assertThat(serializer.fromRow(binaryEntry.fullRow()))
                            .isEqualTo(entries.get(position++));
                }
            }
        }

        assertThat(position).isEqualTo(entries.size());
    }

    @Test
    void testScanProjectedManifestKeepsEntryValidWhenAdvancing() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);

        try (CloseableIterator<ProjectedManifestEntry> iterator =
                manifestFile.scan(manifest.fileName(), projection(DataFileMeta.FILE_NAME))) {
            assertThat(iterator.hasNext()).isTrue();
            ProjectedManifestEntry first = iterator.next();
            assertThat(first.fileName()).isEqualTo(entries.get(0).fileName());

            assertThat(iterator.hasNext()).isTrue();
            ProjectedManifestEntry second = iterator.next();
            assertThat(second).isNotSameAs(first);
            assertThat(second.fileName()).isEqualTo(entries.get(1).fileName());
            assertThat(first.fileName()).isEqualTo(entries.get(0).fileName());
        }
    }

    @Test
    void testScanProjectedManifestCanStopEarly() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        List<String> processedFileNames = new ArrayList<>();

        try (CloseableIterator<ProjectedManifestEntry> iterator =
                manifestFile.scan(manifest.fileName(), projection(DataFileMeta.FILE_NAME))) {
            while (iterator.hasNext()) {
                ProjectedManifestEntry entry = iterator.next();
                processedFileNames.add(entry.fileName());
                break;
            }
        }

        assertThat(processedFileNames).containsExactly(entries.get(0).fileName());
    }

    @Test
    void testScanProjectedManifestKeepsEntryWhenProcessingFails() {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest = writeSingleManifest(manifestFile, entries);
        RuntimeException failure = new RuntimeException("Expected processing failure.");
        List<String> processedFileNames = new ArrayList<>();

        assertThatThrownBy(
                        () -> {
                            try (CloseableIterator<ProjectedManifestEntry> iterator =
                                    manifestFile.scan(
                                            manifest.fileName(),
                                            projection(DataFileMeta.FILE_NAME))) {
                                assertThat(iterator.hasNext()).isTrue();
                                ProjectedManifestEntry entry = iterator.next();
                                processedFileNames.add(entry.fileName());
                                throw failure;
                            }
                        })
                .isSameAs(failure);
        assertThat(processedFileNames).containsExactly(entries.get(0).fileName());
    }

    private List<ManifestEntry> generateData() {
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            entries.add(gen.next());
        }
        return entries;
    }

    private ManifestAvroReader openManifestReader(ManifestFileMeta manifest) throws IOException {
        FileIO fileIO = LocalFileIO.create();
        Path path = new Path(new Path(tempDir.toUri()), "manifest/" + manifest.fileName());
        return new ManifestAvroReader(fileIO.newInputStream(path));
    }

    private void lowerFirstBlockRecordCount(Path path) throws IOException {
        java.nio.file.Path localPath = java.nio.file.Paths.get(path.toUri());
        byte[] bytes = java.nio.file.Files.readAllBytes(localPath);
        byte[] syncMarker = Arrays.copyOfRange(bytes, bytes.length - 16, bytes.length);
        int headerSyncPosition = indexOf(bytes, syncMarker, 4, bytes.length - syncMarker.length);
        assertThat(headerSyncPosition).isGreaterThanOrEqualTo(0);

        int blockCountPosition = headerSyncPosition + syncMarker.length;
        assertThat(bytes[blockCountPosition]).isEqualTo((byte) 4);
        bytes[blockCountPosition] = 2;
        java.nio.file.Files.write(localPath, bytes);
    }

    private static int indexOf(byte[] bytes, byte[] target, int from, int limit) {
        for (int position = from; position + target.length <= limit; position++) {
            int index = 0;
            while (index < target.length && bytes[position + index] == target[index]) {
                index++;
            }
            if (index == target.length) {
                return position;
            }
        }
        return -1;
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
                        new FileSystemSchemaManager(fileIO, path),
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

    private ProjectedManifestEntry.Projection projection(String... projectedFileFields) {
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        List<DataField> fields =
                Arrays.asList(
                        manifestType.getField(ManifestEntry.KIND),
                        manifestType.getField(ManifestEntry.PARTITION),
                        manifestType
                                .getField(ManifestEntry.FILE)
                                .newType(DataFileMeta.SCHEMA.project(projectedFileFields)));
        return ProjectedManifestEntry.Projection.create(new RowType(false, fields));
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
