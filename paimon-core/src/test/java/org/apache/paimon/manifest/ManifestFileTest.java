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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaWriteColsLegacySerializer;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.TestKeyValueGenerator.DEFAULT_PART_TYPE;
import static org.apache.paimon.manifest.ManifestIndexTestUtils.withExtraFiles;
import static org.apache.paimon.stats.StatsTestUtils.convertWithoutSchemaEvolution;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
    void testDeleteManifestAndOnlyReferencedExtraFiles() throws Exception {
        ManifestFile manifests = createManifestFile(tempDir.toString());
        ManifestFileMeta meta = manifests.write(Collections.singletonList(gen.next())).get(0);
        java.nio.file.Path manifestDir = tempDir.resolve("manifest");
        String sidecar = "custom-index" + ManifestSidecar.SUFFIX;
        String extra = "other-extra";
        String unreferenced = meta.fileName() + ManifestSidecar.SUFFIX;
        for (String name : Arrays.asList(sidecar, extra, unreferenced)) {
            Files.createFile(manifestDir.resolve(name));
        }
        manifests.delete(
                ManifestIndexTestUtils.withExtraFiles(meta, Arrays.asList(sidecar, extra)));
        assertThat(Files.exists(manifestDir.resolve(meta.fileName()))).isFalse();
        assertThat(Files.exists(manifestDir.resolve(sidecar))).isFalse();
        assertThat(Files.exists(manifestDir.resolve(extra))).isFalse();
        assertThat(Files.exists(manifestDir.resolve(unreferenced))).isTrue();
        // Deleting already removed files is harmless.
        manifests.delete(
                ManifestIndexTestUtils.withExtraFiles(meta, Arrays.asList(sidecar, extra)));
    }

    @Test
    void testWriteManifestFileToExplicitPath() throws Exception {
        List<ManifestEntry> entries = generateData();
        ManifestFile manifestFile = createManifestFile(tempDir.toString());
        Path path = new Path(tempDir.toString() + "/manifest/explicit-manifest");

        ManifestAvroWriter writer = manifestFile.createAvroWriter(path);
        writer.write(entries);
        writer.close();

        assertThat(writer.result())
                .singleElement()
                .extracting(ManifestFileMeta::fileName)
                .isEqualTo("explicit-manifest");
        assertThat(manifestFile.read("explicit-manifest")).isEqualTo(entries);
    }

    @Test
    void testAbortedManifestWriterDoesNotExposeResults() throws Exception {
        ManifestAvroWriter writer = createManifestFile(tempDir.toString()).createAvroWriter();
        writer.write(gen.next());

        writer.abort();

        assertThatThrownBy(writer::result)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before closing");
    }

    @Test
    void testWriteEncodedMixedBlockCountsDeletes() throws Exception {
        assertEncodedBlockCounts(FileKind.ADD, FileKind.DELETE);
    }

    @Test
    void testWriteEncodedDeleteOnlyBlockCountsDeletes() throws Exception {
        assertEncodedBlockCounts(FileKind.DELETE, FileKind.DELETE);
    }

    @Test
    void testWriteEncodedBlockWithoutRowIds() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta sourceMeta = writeSingleManifest(manifestFile, entries);
        assertThat(sourceMeta.minRowId()).isNull();
        assertThat(sourceMeta.maxRowId()).isNull();

        ManifestAvroWriter writer = manifestFile.createAvroWriter();
        try (ManifestAvroReader reader = openManifestReader(sourceMeta)) {
            assertThat(reader.hasNext()).isTrue();
            ManifestAvroReader.RawBlock block = reader.next();
            writer.writeEncodedBlock(block.encodedBlock(), encodedBlockMeta(sourceMeta));
            assertThat(reader.hasNext()).isFalse();
        }
        writer.close();

        ManifestFileMeta result = writer.result().get(0);
        assertThat(result.minRowId()).isNull();
        assertThat(result.maxRowId()).isNull();
        assertThat(manifestFile.read(result.fileName())).containsExactlyElementsOf(entries);
    }

    @Test
    void testWriteEncodedRecords() throws Exception {
        ManifestEntry source = gen.next();
        List<ManifestEntry> entries =
                Arrays.asList(
                        ManifestEntry.create(
                                FileKind.ADD,
                                source.partition(),
                                source.bucket(),
                                source.totalBuckets(),
                                source.file().newFirstRowId(10L)),
                        ManifestEntry.create(
                                FileKind.DELETE,
                                source.partition(),
                                source.bucket(),
                                source.totalBuckets(),
                                source.file().newFirstRowId(20L)));
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta sourceMeta = writeSingleManifest(manifestFile, entries);
        ManifestAvroWriter writer = manifestFile.createAvroWriter();
        ManifestAvroWriter.EncodedEntry metadata = new ManifestAvroWriter.EncodedEntry();
        int position = 0;

        try (ManifestAvroReader reader = openManifestReader(sourceMeta)) {
            while (reader.hasNext()) {
                ManifestAvroReader.RowIterator rows =
                        reader.next().toRows(ManifestEntry.MANIFEST_ROW_TYPE);
                while (rows.hasNext()) {
                    rows.next();
                    ManifestEntry entry = entries.get(position++);
                    writer.writeEncoded(
                            rows.encodedRecord(),
                            metadata.replace(
                                    entry.kind().toByteValue(),
                                    entry.partition(),
                                    entry.bucket(),
                                    entry.totalBuckets(),
                                    entry.level(),
                                    entry.file().schemaId(),
                                    entry.file().firstRowId(),
                                    entry.file().rowCount()));
                }
            }
        }
        writer.close();

        assertThat(position).isEqualTo(entries.size());
        ManifestFileMeta result = writer.result().get(0);
        assertThat(result.numAddedFiles()).isEqualTo(sourceMeta.numAddedFiles());
        assertThat(result.numDeletedFiles()).isEqualTo(sourceMeta.numDeletedFiles());
        assertThat(result.partitionStats()).isEqualTo(sourceMeta.partitionStats());
        assertThat(result.schemaId()).isEqualTo(sourceMeta.schemaId());
        assertThat(result.minBucket()).isEqualTo(sourceMeta.minBucket());
        assertThat(result.maxBucket()).isEqualTo(sourceMeta.maxBucket());
        assertThat(result.totalBuckets()).isEqualTo(sourceMeta.totalBuckets());
        assertThat(result.minLevel()).isEqualTo(sourceMeta.minLevel());
        assertThat(result.maxLevel()).isEqualTo(sourceMeta.maxLevel());
        assertThat(result.minRowId()).isEqualTo(sourceMeta.minRowId());
        assertThat(result.maxRowId()).isEqualTo(sourceMeta.maxRowId());
        assertThat(manifestFile.read(result.fileName())).containsExactlyElementsOf(entries);
    }

    @Test
    void testWriteEncodedRecordsFlushesPartitionStatsBuffer() throws Exception {
        ManifestEntry generated = gen.next();
        ManifestEntry source =
                ManifestEntry.create(
                        generated.kind(),
                        generated.partition(),
                        generated.bucket(),
                        generated.totalBuckets(),
                        generated.file().newFirstRowId(0L));
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta sourceMeta =
                writeSingleManifest(manifestFile, Collections.singletonList(source));
        ManifestAvroWriter writer = manifestFile.createAvroWriter();
        ManifestAvroWriter.EncodedEntry metadata = new ManifestAvroWriter.EncodedEntry();
        int recordCount = 8_200;

        try (ManifestAvroReader reader = openManifestReader(sourceMeta)) {
            ManifestAvroReader.RowIterator rows =
                    reader.next().toRows(ManifestEntry.MANIFEST_ROW_TYPE);
            rows.next();
            ByteBuffer encodedRecord = rows.encodedRecord();
            for (int i = 0; i < recordCount; i++) {
                writer.writeEncoded(
                        encodedRecord.duplicate(),
                        metadata.replace(
                                source.kind().toByteValue(),
                                source.partition().copy(),
                                source.bucket(),
                                source.totalBuckets(),
                                source.level(),
                                source.file().schemaId(),
                                source.file().firstRowId(),
                                source.file().rowCount()));
            }
        }
        writer.close();

        ManifestFileMeta result = writer.result().get(0);
        assertThat(result.numAddedFiles()).isEqualTo(recordCount);
        assertThat(result.numDeletedFiles()).isZero();
        assertThat(result.partitionStats()).isEqualTo(sourceMeta.partitionStats());
    }

    @Test
    void testWriteEncodedManifestPreservesUnknownAggregateStats() throws Exception {
        List<ManifestEntry> entries = Arrays.asList(gen.next(), gen.next());
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta source = writeSingleManifest(manifestFile, entries);
        ManifestFileMeta unknownStats =
                new ManifestFileMeta(
                        source.fileName(),
                        source.fileSize(),
                        source.numAddedFiles(),
                        source.numDeletedFiles(),
                        source.partitionStats(),
                        source.schemaId(),
                        null,
                        null,
                        null,
                        null,
                        source.minRowId(),
                        source.maxRowId(),
                        null,
                        null);

        ManifestAvroWriter writer = manifestFile.createAvroWriter();
        try (ManifestAvroReader reader = openManifestReader(source)) {
            writer.writeEncodedManifest(reader, unknownStats);
        }
        writer.close();

        ManifestFileMeta result = writer.result().get(0);
        assertThat(result.minBucket()).isNull();
        assertThat(result.maxBucket()).isNull();
        assertThat(result.totalBuckets()).isNull();
        assertThat(result.minLevel()).isNull();
        assertThat(result.maxLevel()).isNull();
        assertThat(result.partitionStats()).isEqualTo(source.partitionStats());
        assertThat(manifestFile.read(result.fileName())).containsExactlyElementsOf(entries);
    }

    @Test
    void testTotalBucketsAggregateStats() throws Exception {
        ManifestEntry source = gen.next();
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);

        ManifestEntry add =
                ManifestEntry.create(
                        FileKind.ADD, source.partition(), source.bucket(), 8, source.file());
        ManifestEntry delete =
                ManifestEntry.create(
                        FileKind.DELETE, source.partition(), source.bucket(), 8, source.file());
        assertThat(writeSingleManifest(manifestFile, Arrays.asList(add, delete)).totalBuckets())
                .isEqualTo(8);

        ManifestEntry different =
                ManifestEntry.create(
                        FileKind.ADD, source.partition(), source.bucket(), 16, source.file());
        assertThat(writeSingleManifest(manifestFile, Arrays.asList(add, different)).totalBuckets())
                .isNull();

        ManifestEntry nonPositive =
                ManifestEntry.create(
                        FileKind.DELETE, source.partition(), source.bucket(), 0, source.file());
        assertThat(
                        writeSingleManifest(manifestFile, Arrays.asList(add, nonPositive))
                                .totalBuckets())
                .isNull();
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

    @ParameterizedTest
    @MethodSource("reorderedManifestFieldOrders")
    void testProjectedScanRejectsUnsupportedFormatIdentifier(
            int[] fieldOrder, boolean reorderNestedFields) throws Exception {
        ManifestEntry entry = gen.next();
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta manifest =
                writeSingleManifest(manifestFile, Collections.singletonList(entry));
        Path path = new Path(new Path(tempDir.toUri()), "manifest/" + manifest.fileName());
        LocalFileIO fileIO = LocalFileIO.create();
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();
        GenericRow invalid = (GenericRow) serializer.toRow(entry);
        invalid.setField(0, 1);
        RowType writerType = reorderedManifestType(fieldOrder, reorderNestedFields);

        try (PositionOutputStream out = fileIO.newOutputStream(path, true);
                FormatWriter writer = avro.createWriterFactory(writerType).create(out, "zstd")) {
            writer.addElement(reorderRow(invalid, ManifestEntry.MANIFEST_ROW_TYPE, writerType));
        }

        try (CloseableIterator<ProjectedManifestEntry> entries =
                manifestFile.scan(
                        manifest.fileName(), ProjectedManifestEntry.DELETE_ENTRY_PROJECTION)) {
            assertThat(entries.hasNext()).isTrue();
            assertThatThrownBy(entries::next)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not compatible");
        }

        // An unprojected version must still be checked even when every row is filtered out.
        try (ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path));
                CloseableIterator<InternalRow> rows =
                        reader.read(
                                new RowType(false, Collections.emptyList()),
                                null,
                                new BucketFilter(false, null, bucket -> false, null))) {
            assertThatThrownBy(rows::hasNext)
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
    void testLegacyAvroReaderSkipsColumnSequenceNumbers() throws Exception {
        ManifestEntry expected = gen.next();
        ManifestEntry source =
                ManifestEntry.create(
                        expected.kind(),
                        expected.partition(),
                        expected.bucket(),
                        expected.totalBuckets(),
                        expected.file().withColumnMaxSequenceNumbers(new long[] {3L, 42L}));
        List<DataField> legacyManifestFields =
                ManifestEntry.MANIFEST_ROW_TYPE.getFields().stream()
                        .map(
                                field ->
                                        ManifestEntry.FILE.equals(field.name())
                                                ? field.newType(
                                                        DataFileMetaWriteColsLegacySerializer
                                                                .SCHEMA)
                                                : field)
                        .collect(Collectors.toList());
        RowType legacyManifestType = new RowType(false, legacyManifestFields);
        Path path = new Path(new Path(tempDir.toUri()), "new-manifest.avro");
        LocalFileIO fileIO = LocalFileIO.create();
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();

        try (PositionOutputStream out = fileIO.newOutputStream(path, false);
                FormatWriter writer =
                        avro.createWriterFactory(ManifestEntry.MANIFEST_ROW_TYPE)
                                .create(out, "zstd")) {
            writer.addElement(serializer.toRow(source));
        }

        ManifestEntry actual;
        try (ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path));
                CloseableIterator<InternalRow> rows = reader.read(legacyManifestType, null, null)) {
            assertThat(rows.hasNext()).isTrue();
            actual = new ManifestEntryWriteColsLegacySerializer().fromRow(rows.next());
            assertThat(rows.hasNext()).isFalse();
        }

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.file().columnMaxSequenceNumbers()).isNull();
    }

    @ParameterizedTest
    @MethodSource("manifestWriterSchemas")
    void testAvroReaderSupportsReorderedWriterFields(
            int[] fieldOrder, boolean reorderNestedFields, boolean nullableFile) throws Exception {
        List<ManifestEntry> entries = generateData();
        RowType writerType = reorderedManifestType(fieldOrder, reorderNestedFields);
        if (nullableFile) {
            writerType = nullableFileType(writerType);
        }
        Path path = new Path(new Path(tempDir.toUri()), "reordered-manifest.avro");
        LocalFileIO fileIO = LocalFileIO.create();
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();
        try (PositionOutputStream out = fileIO.newOutputStream(path, false);
                FormatWriter writer = avro.createWriterFactory(writerType).create(out, "zstd")) {
            for (ManifestEntry entry : entries) {
                writer.addElement(
                        reorderRow(
                                serializer.toRow(entry),
                                ManifestEntry.MANIFEST_ROW_TYPE,
                                writerType));
            }
        }

        boolean rawCopySupported =
                !nullableFile
                        && !reorderNestedFields
                        && Arrays.equals(fieldOrder, new int[] {0, 1, 2, 3, 4, 5});
        List<InternalRow> retained = new ArrayList<>();
        try (ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path));
                CloseableIterator<InternalRow> rows =
                        reader.read(ManifestEntry.MANIFEST_ROW_TYPE, null, null)) {
            assertThat(reader.rawBlockCopySupported()).isEqualTo(rawCopySupported);
            while (rows.hasNext()) {
                retained.add(rows.next());
            }
        }
        assertThat(retained.stream().map(serializer::fromRow).collect(Collectors.toList()))
                .containsExactlyElementsOf(entries);

        // Raw blocks must also decode into canonical rows, including when reusing a row.
        List<ManifestEntry> decoded = new ArrayList<>();
        try (ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path))) {
            while (reader.hasNext()) {
                ManifestAvroReader.RawBlock block = reader.next().stableCopy();
                assertThat(block.rawBlockCopySupported()).isEqualTo(rawCopySupported);
                ManifestAvroReader.RowIterator rows = block.toRows(ManifestEntry.MANIFEST_ROW_TYPE);
                while (rows.hasNext()) {
                    decoded.add(serializer.fromRow(rows.next()));
                }
            }
        }
        assertThat(decoded).containsExactlyElementsOf(entries);

        ManifestEntry selected = entries.get(0);
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromMultiple(
                        DEFAULT_PART_TYPE, Collections.singletonList(selected.partition()));
        BucketFilter bucketFilter =
                new BucketFilter(
                        false,
                        null,
                        null,
                        (partition, bucket, totalBuckets) ->
                                partition.equals(selected.partition())
                                        && bucket == selected.bucket()
                                        && totalBuckets == selected.totalBuckets());
        List<DataField> fields = ManifestEntry.MANIFEST_ROW_TYPE.getFields();
        RowType fileProjection =
                new RowType(
                        false,
                        Collections.singletonList(
                                fields.get(5)
                                        .newType(
                                                DataFileMeta.SCHEMA.project(
                                                        DataFileMeta.ROW_COUNT,
                                                        DataFileMeta.FILE_NAME))));
        RowType kindProjection = new RowType(false, Collections.singletonList(fields.get(1)));
        // Filter fields need not be projected, and unprojected file metadata must be skipped.
        for (RowType projectedType : Arrays.asList(fileProjection, kindProjection)) {
            for (PartitionPredicate filter : Arrays.asList(null, partitionFilter)) {
                for (BucketFilter buckets : Arrays.asList(null, bucketFilter)) {
                    List<ManifestEntry> expected =
                            entries.stream()
                                    .filter(e -> filter == null || filter.test(e.partition()))
                                    .filter(
                                            e ->
                                                    buckets == null
                                                            || buckets.test(
                                                                    e.partition(),
                                                                    e.bucket(),
                                                                    e.totalBuckets()))
                                    .collect(Collectors.toList());
                    try (ManifestAvroReader reader =
                                    new ManifestAvroReader(fileIO.newInputStream(path));
                            CloseableIterator<InternalRow> rows =
                                    reader.read(projectedType, filter, buckets)) {
                        for (ManifestEntry entry : expected) {
                            assertThat(rows.hasNext()).isTrue();
                            InternalRow row = rows.next();
                            if (projectedType == kindProjection) {
                                assertThat(row.getByte(0)).isEqualTo(entry.kind().toByteValue());
                            } else {
                                InternalRow file = row.getRow(0, 2);
                                assertThat(file.getLong(0)).isEqualTo(entry.rowCount());
                                assertThat(file.getString(1).toString())
                                        .isEqualTo(entry.fileName());
                            }
                        }
                        assertThat(rows.hasNext()).isFalse();
                    }
                }
            }
        }
    }

    @Test
    void testNullableFileRecordWithManifestCache() throws Exception {
        List<ManifestEntry> entries = generateData();
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(new Path(tempDir.toString()), "manifest/nullable-manifest.avro");
        fileIO.mkdirs(path.getParent());
        ManifestEntrySerializer serializer = new ManifestEntrySerializer();
        try (PositionOutputStream out = fileIO.newOutputStream(path, false);
                FormatWriter writer =
                        avro.createWriterFactory(nullableFileType(ManifestEntry.MANIFEST_ROW_TYPE))
                                .create(out, "zstd")) {
            for (ManifestEntry entry : entries) {
                writer.addElement(serializer.toRow(entry));
            }
        }

        SegmentsCache<Path> cache =
                new SegmentsCache<>(32 * 1024, new MemorySize(4 * 1024 * 1024), 1024 * 1024);
        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE, cache);
        assertThat(manifestFile.read(path.getName())).containsExactlyInAnyOrderElementsOf(entries);
        assertThat(cache.getIfPresents(path)).isNotNull();
        assertThat(manifestFile.read(path.getName())).containsExactlyInAnyOrderElementsOf(entries);
    }

    @ParameterizedTest
    @MethodSource("invalidManifestFieldTypes")
    void testRejectsInvalidManifestFieldTypes(String fieldName, DataType fieldType)
            throws Exception {
        RowType writerType =
                new RowType(
                        false,
                        ManifestEntry.MANIFEST_ROW_TYPE.getFields().stream()
                                .map(
                                        field ->
                                                field.name().equals(fieldName)
                                                        ? field.newType(fieldType)
                                                        : field)
                                .collect(Collectors.toList()));
        GenericRow row = (GenericRow) new ManifestEntrySerializer().toRow(gen.next());
        if (ManifestEntry.FILE.equals(fieldName)) {
            row.setField(writerType.getFieldIndex(fieldName), 42);
        }
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(new Path(tempDir.toUri()), "invalid-field.avro");
        try (PositionOutputStream out = fileIO.newOutputStream(path, false);
                FormatWriter writer = avro.createWriterFactory(writerType).create(out, "zstd")) {
            writer.addElement(row);
        }

        for (RowType projectedType :
                Arrays.asList(
                        ManifestEntry.MANIFEST_ROW_TYPE,
                        ManifestEntry.MANIFEST_ROW_TYPE.project(ManifestEntry.KIND))) {
            try (ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path));
                    CloseableIterator<InternalRow> rows = reader.read(projectedType, null, null)) {
                assertThatThrownBy(rows::hasNext)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining(
                                "Unexpected Manifest Avro type for field " + fieldName);
            }
        }
    }

    private static Stream<Arguments> invalidManifestFieldTypes() {
        return Stream.of(
                Arguments.of(ManifestEntry.FILE, DataTypes.INT().notNull()),
                Arguments.of(ManifestEntry.FILE, DataTypes.INT()),
                Arguments.of(ManifestEntry.BUCKET, DataTypes.INT()),
                Arguments.of("_VERSION", DataTypes.INT()),
                Arguments.of(
                        ManifestEntry.PARTITION,
                        ManifestEntry.MANIFEST_ROW_TYPE
                                .getField(ManifestEntry.PARTITION)
                                .type()
                                .copy(true)));
    }

    private static RowType nullableFileType(RowType type) {
        return new RowType(
                type.isNullable(),
                type.getFields().stream()
                        .map(
                                field ->
                                        ManifestEntry.FILE.equals(field.name())
                                                ? field.newType(field.type().copy(true))
                                                : field)
                        .collect(Collectors.toList()));
    }

    private static Stream<Arguments> manifestWriterSchemas() {
        return reorderedManifestFieldOrders()
                .flatMap(
                        arguments ->
                                Stream.of(
                                        Arguments.of(arguments.get()[0], arguments.get()[1], false),
                                        Arguments.of(
                                                arguments.get()[0], arguments.get()[1], true)));
    }

    private static Stream<Arguments> reorderedManifestFieldOrders() {
        return Stream.of(
                        new int[] {0, 1, 2, 3, 4, 5},
                        new int[] {0, 5, 1, 2, 3, 4},
                        new int[] {1, 2, 3, 4, 5, 0},
                        new int[] {5, 4, 3, 2, 1, 0},
                        new int[] {3, 0, 5, 1, 4, 2},
                        new int[] {1, 0, 2, 4, 3, 5})
                .flatMap(order -> Stream.of(Arguments.of(order, false), Arguments.of(order, true)));
    }

    private static RowType reorderedManifestType(int[] fieldOrder, boolean reorderNestedFields) {
        List<DataField> fileFields = new ArrayList<>(DataFileMeta.SCHEMA.getFields());
        Collections.reverse(fileFields);
        List<DataField> fields = ManifestEntry.MANIFEST_ROW_TYPE.getFields();
        return new RowType(
                false,
                Arrays.stream(fieldOrder)
                        .mapToObj(fields::get)
                        .map(
                                field ->
                                        reorderNestedFields
                                                        && ManifestEntry.FILE.equals(field.name())
                                                ? field.newType(new RowType(false, fileFields))
                                                : field)
                        .collect(Collectors.toList()));
    }

    private static GenericRow reorderRow(InternalRow row, RowType sourceType, RowType targetType) {
        GenericRow result = new GenericRow(targetType.getFieldCount());
        for (int i = 0; i < targetType.getFieldCount(); i++) {
            DataField field = targetType.getFields().get(i);
            int sourcePosition = sourceType.getFieldIndex(field.name());
            Object value =
                    InternalRow.createFieldGetter(
                                    sourceType.getTypeAt(sourcePosition), sourcePosition)
                            .getFieldOrNull(row);
            if (value != null && field.type() instanceof RowType) {
                value =
                        reorderRow(
                                (InternalRow) value,
                                (RowType) sourceType.getTypeAt(sourcePosition),
                                (RowType) field.type());
            }
            result.setField(i, value);
        }
        return result;
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
        byte[] bytes = Files.readAllBytes(tempDir.resolve("manifest").resolve(manifest.fileName()));
        int split = bytes.length / 2;

        try (ManifestAvroReader reader =
                new ManifestAvroReader(
                        new SequenceInputStream(
                                new ByteArrayInputStream(bytes, 0, split),
                                new ByteArrayInputStream(bytes, split, bytes.length - split)))) {
            byte[] header = reader.headerBytes();
            assertThat(header).isEqualTo(Arrays.copyOf(bytes, header.length));
            long nextOffset = header.length;
            while (reader.hasNext()) {
                ManifestAvroReader.RawBlock block = reader.next();
                assertThat(reader.blockOffset()).isEqualTo(nextOffset);
                assertThat(reader.blockLength()).isPositive();
                nextOffset += reader.blockLength();
                ManifestAvroReader.RowIterator rows = block.toRows(projection.projectedType());
                assertThat(rows.hasNext()).isTrue();
                while (rows.hasNext()) {
                    rows.next();
                    rowCount++;
                }
                blockCount++;
            }
            assertThat(nextOffset).isEqualTo(bytes.length);
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

    private void assertEncodedBlockCounts(FileKind... kinds) throws Exception {
        ManifestEntry source = gen.next();
        List<ManifestEntry> entries = new ArrayList<>();
        long firstRowId = 10;
        for (FileKind kind : kinds) {
            DataFileMeta file = source.file().newFirstRowId(firstRowId);
            entries.add(
                    ManifestEntry.create(
                            kind,
                            source.partition(),
                            source.bucket(),
                            source.totalBuckets(),
                            file));
            firstRowId += file.rowCount();
        }

        ManifestFile manifestFile = createManifestFile(tempDir.toString(), Long.MAX_VALUE);
        ManifestFileMeta sourceMeta = writeSingleManifest(manifestFile, entries);
        ManifestAvroWriter writer = manifestFile.createAvroWriter();
        try (ManifestAvroReader reader = openManifestReader(sourceMeta)) {
            assertThat(reader.hasNext()).isTrue();
            ManifestAvroReader.RawBlock block = reader.next();
            assertThat(block.recordCount()).isEqualTo(entries.size());
            writer.writeEncodedBlock(block.encodedBlock(), encodedBlockMeta(sourceMeta));
            assertThat(reader.hasNext()).isFalse();
        }
        writer.close();

        ManifestFileMeta result = writer.result().get(0);
        assertThat(result.numAddedFiles())
                .isEqualTo(entries.stream().filter(entry -> entry.kind() == FileKind.ADD).count());
        assertThat(result.numDeletedFiles())
                .isEqualTo(
                        entries.stream().filter(entry -> entry.kind() == FileKind.DELETE).count());
        assertThat(manifestFile.read(result.fileName())).containsExactlyElementsOf(entries);
    }

    private ManifestAvroWriter.EncodedBlockMeta encodedBlockMeta(ManifestFileMeta meta) {
        return new ManifestAvroWriter.EncodedBlockMeta(
                meta.numAddedFiles(),
                meta.numDeletedFiles(),
                meta.schemaId(),
                meta.minBucket(),
                meta.maxBucket(),
                meta.totalBuckets(),
                meta.minLevel(),
                meta.maxLevel(),
                meta.minRowId() == null ? -1 : meta.minRowId(),
                meta.maxRowId() == null ? -1 : meta.maxRowId(),
                meta.partitionStats());
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

    @Test
    void testSidecarCacheUsesExplicitPathsAndIsSeparateFromManifestCache() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        RecordingFileIO io = new RecordingFileIO();
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), Long.MAX_VALUE, null, false);
        SegmentsCache<Path> sidecarCache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), Long.MAX_VALUE, null, false);
        ManifestFile.Factory factory =
                createManifestFileFactory(
                        tempDir.toString(), Long.MAX_VALUE, options, io, cache, sidecarCache);
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ManifestEntry entry = gen.next();
            entries.add(
                    ManifestEntry.create(
                            FileKind.ADD,
                            entry.partition(),
                            entry.bucket(),
                            entry.totalBuckets(),
                            entry.file().newFirstRowId(i * 1000000L)));
        }
        ManifestFileMeta written = factory.create().write(entries).get(0);
        String sidecarName = "cached-explicit" + ManifestSidecar.SUFFIX;
        java.nio.file.Path sidecar = tempDir.resolve("manifest").resolve(sidecarName);
        java.nio.file.Files.move(
                tempDir.resolve("manifest").resolve(ManifestSidecar.fileName(written)), sidecar);
        ManifestFileMeta meta = withExtraFiles(written, Collections.singletonList(sidecarName));
        Path sidecarPath = new Path(tempDir.toString(), "manifest/" + sidecarName);

        io.reset();
        assertThat(
                        factory.create()
                                .selectBlocks(
                                        meta,
                                        RowRangeIndex.create(
                                                Collections.singletonList(
                                                        new Range(Long.MAX_VALUE, Long.MAX_VALUE))))
                                .blocks())
                .isEmpty();
        assertThat(io.opened).containsExactly(sidecarPath);
        assertThat(sidecarCache.getIfPresents(sidecarPath).totalMemorySize())
                .isEqualTo(java.nio.file.Files.size(sidecar));
        assertThat(cache.estimatedSize()).isZero();

        io.reset();
        RowRangeIndex hit = RowRangeIndex.create(Collections.singletonList(new Range(0, 0)));
        assertThat(factory.create().selectBlocks(meta, hit).blocks()).isNotEmpty();
        assertThat(io.opened).isEmpty();
        assertThat(factory.create().read(meta.fileName()))
                .containsExactlyInAnyOrderElementsOf(entries);
        assertThat(cache.estimatedSize()).isEqualTo(1);
        assertThat(sidecarCache.estimatedSize()).isEqualTo(1);
        assertThat(cache.getIfPresents(sidecarPath)).isNull();
        assertThat(
                        sidecarCache.getIfPresents(
                                new Path(tempDir.toString(), "manifest/" + meta.fileName())))
                .isNull();

        io.reset();
        assertThat(factory.create().selectBlocks(meta, hit).blocks()).isNotEmpty();
        assertThat(factory.create().read(meta.fileName()))
                .containsExactlyInAnyOrderElementsOf(entries);
        assertThat(io.opened).isEmpty();
    }

    @Test
    void testDedicatedSidecarCacheAndManifestCacheFallback() {
        Options options = new Options();
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        for (boolean cacheManifest : new boolean[] {false, true}) {
            for (boolean cacheSidecar : new boolean[] {false, true}) {
                RecordingFileIO io = new RecordingFileIO();
                SegmentsCache<Path> manifestCache =
                        cacheManifest
                                ? new SegmentsCache<>(
                                        1024,
                                        MemorySize.ofMebiBytes(1),
                                        Long.MAX_VALUE,
                                        null,
                                        false)
                                : null;
                SegmentsCache<Path> sidecarCache =
                        cacheSidecar
                                ? new SegmentsCache<>(
                                        1024,
                                        MemorySize.ofMebiBytes(1),
                                        Long.MAX_VALUE,
                                        null,
                                        false)
                                : null;
                ManifestFile.Factory factory =
                        createManifestFileFactory(
                                tempDir.resolve(cacheManifest + "-" + cacheSidecar).toString(),
                                Long.MAX_VALUE,
                                options,
                                io,
                                manifestCache,
                                sidecarCache);
                ManifestEntry entry = gen.next();
                ManifestEntry added =
                        ManifestEntry.create(
                                FileKind.ADD,
                                entry.partition(),
                                entry.bucket(),
                                entry.totalBuckets(),
                                entry.file().newFirstRowId(0L));
                ManifestFileMeta meta =
                        factory.create().write(Collections.singletonList(added)).get(0);
                RowRangeIndex query =
                        RowRangeIndex.create(Collections.singletonList(new Range(0, 0)));

                for (int round = 0; round < 2; round++) {
                    io.reset();
                    ManifestFile manifest = factory.create();
                    ManifestSidecar.Selection selected = manifest.selectBlocks(meta, query);
                    assertThat(readSelectedEntries(manifest, meta, selected))
                            .containsExactly(added);
                    assertThat(
                                    io.opened.stream()
                                            .filter(
                                                    path ->
                                                            path.getName()
                                                                    .endsWith(
                                                                            ManifestSidecar
                                                                                    .SUFFIX)))
                            .hasSize(round == 0 || (!cacheSidecar && !cacheManifest) ? 1 : 0);
                    assertThat(
                                    io.opened.stream()
                                            .filter(path -> path.getName().equals(meta.fileName())))
                            .hasSize(round == 0 || !cacheManifest ? 1 : 0);
                }
                if (manifestCache != null) {
                    // Blocks stay in the manifest cache; sidecar bytes only join them on fallback.
                    assertThat(manifestCache.estimatedSize()).isEqualTo(cacheSidecar ? 1 : 2);
                }
                if (sidecarCache != null) {
                    assertThat(sidecarCache.estimatedSize()).isEqualTo(1);
                }
            }
        }
    }

    @Test
    void testReadsOnlySelectedBlocksAndPreservesPhysicalOrdinals() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        RecordingFileIO fileIO = new RecordingFileIO();
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(16), Long.MAX_VALUE);
        ManifestFile.Factory factory =
                createManifestFileFactory(
                        tempDir.toString(), Long.MAX_VALUE, options, fileIO, cache);
        ManifestFile manifests = factory.create();
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            ManifestEntry entry = gen.next();
            entries.add(
                    ManifestEntry.create(
                            FileKind.ADD,
                            entry.partition(),
                            entry.bucket(),
                            entry.totalBuckets(),
                            entry.file().newFirstRowId(i * 1000000L)));
        }
        ManifestFileMeta meta = manifests.write(entries).get(0);
        Path manifestPath = new Path(tempDir.toString(), "manifest/" + meta.fileName());
        RowRangeIndex query =
                RowRangeIndex.create(
                        Arrays.asList(
                                new Range(1000000000L, 1000000000L),
                                new Range(3000000000L, 3000000000L)));

        ManifestSidecar.Selection selected = manifests.selectBlocks(meta, query);
        assertThat(selected.blocks()).hasSize(2);

        fileIO.reset();
        List<ManifestEntry> actual = readSelectedEntries(factory.create(), meta, selected);
        List<ManifestEntry> expected = new ArrayList<>();
        for (ManifestSidecar.Block block : selected.blocks()) {
            expected.addAll(
                    entries.subList(
                            (int) block.firstRecord,
                            (int) (block.firstRecord + block.recordCount)));
        }
        assertThat(actual).containsExactlyElementsOf(expected);
        assertThat(actual).contains(entries.get(1000), entries.get(3000));
        assertThat(fileIO.bytes.get()).isLessThan(meta.fileSize() / 4);
        assertThat(fileIO.seeks)
                .containsExactlyElementsOf(
                        selected.blocks().stream()
                                .map(block -> block.offset)
                                .collect(Collectors.toList()));
        assertThat(fileIO.opened).containsExactly(manifestPath);
        assertThat(cache.getIfPresents(manifestPath)).isNull();
        fileIO.reset();
        assertThat(readSelectedEntries(factory.create(), meta, selected))
                .containsExactlyElementsOf(expected);
        assertThat(fileIO.opened).isEmpty();
        assertThat(fileIO.bytes.get()).isZero();
        ManifestSidecar.Selection allBlocks =
                manifests.selectBlocks(
                        meta,
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))));
        fileIO.reset();
        assertThat(readSelectedEntries(factory.create(), meta, allBlocks))
                .containsExactlyInAnyOrderElementsOf(entries);
        assertThat(fileIO.opened).containsExactly(manifestPath);
        assertThat(cache.getIfPresents(manifestPath)).isNull();

        fileIO.reset();
        assertThat(readSelectedEntries(factory.create(), meta, allBlocks))
                .containsExactlyInAnyOrderElementsOf(entries);
        assertThat(fileIO.opened).isEmpty();
        assertThat(cache.getIfPresents(manifestPath)).isNull();

        // Reads without a sidecar selection populate and reuse the full-manifest cache.
        assertThat(manifests.read(meta.fileName())).containsExactlyInAnyOrderElementsOf(entries);
        assertThat(fileIO.opened).containsExactly(manifestPath);
        assertThat(cache.getIfPresents(manifestPath)).isNotNull();

        fileIO.reset();
        assertThat(manifests.read(meta.fileName())).containsExactlyInAnyOrderElementsOf(entries);
        assertThat(fileIO.opened).isEmpty();

        long largestBlock =
                allBlocks.blocks().stream().mapToLong(block -> block.length).max().getAsLong();
        assertThat(meta.fileSize()).isGreaterThan(largestBlock);
        SegmentsCache<Path> blockCache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(16), largestBlock, null, false);
        ManifestFile.Factory limitedFactory =
                createManifestFileFactory(
                        tempDir.toString(), Long.MAX_VALUE, options, fileIO, blockCache);
        for (int round = 0; round < 2; round++) {
            fileIO.reset();
            assertThat(readSelectedEntries(limitedFactory.create(), meta, allBlocks))
                    .containsExactlyElementsOf(entries);
            if (round == 1) {
                assertThat(fileIO.opened).isEmpty();
            }
        }
        assertThat(
                        blockCache.getIfPresents(
                                new Path(tempDir.toString(), "manifest/" + meta.fileName())))
                .isNull();
    }

    @Test
    void testScannerPreservesDeletesAndColumnGroups() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        RecordingFileIO fileIO = new RecordingFileIO();
        ManifestFile.Factory factory =
                createManifestFileFactory(tempDir.toString(), Long.MAX_VALUE, options, fileIO);
        ManifestFile manifests = factory.create();
        ManifestEntry entry = gen.next();
        ManifestEntry add =
                ManifestEntry.create(
                        FileKind.ADD,
                        entry.partition(),
                        entry.bucket(),
                        entry.totalBuckets(),
                        entry.file().newFirstRowId(100L));
        ManifestEntry delete =
                ManifestEntry.create(
                        FileKind.DELETE,
                        entry.partition(),
                        entry.bucket(),
                        entry.totalBuckets(),
                        add.file());
        ManifestEntry other = gen.next();
        ManifestEntry live =
                ManifestEntry.create(
                        FileKind.ADD,
                        other.partition(),
                        other.bucket(),
                        other.totalBuckets(),
                        other.file().newFirstRowId(100L));
        List<ManifestFileMeta> metas = new ArrayList<>();
        metas.addAll(manifests.write(Arrays.asList(add, live)));
        metas.addAll(manifests.write(Collections.singletonList(delete)));
        metas.addAll(
                manifests.write(
                        Collections.singletonList(
                                ManifestEntry.create(
                                        FileKind.ADD,
                                        entry.partition(),
                                        entry.bucket(),
                                        entry.totalBuckets(),
                                        entry.file().newFirstRowId(0L)))));
        AppendOnlyFileStoreScan scan =
                new AppendOnlyFileStoreScan(
                        mock(ManifestsReader.class),
                        null,
                        null,
                        null,
                        mock(TableSchema.class),
                        factory,
                        2,
                        false,
                        false,
                        false);
        scan.withRowRanges(Collections.singletonList(new Range(100, 100)));
        fileIO.reset();
        List<ManifestEntry> result = new ArrayList<>();
        scan.readManifestEntries(metas, false).forEachRemaining(result::add);
        assertThat(result).containsExactly(live);
        assertThat(
                        fileIO.opened.stream()
                                .filter(path -> !path.getName().endsWith(ManifestSidecar.SUFFIX))
                                .map(Path::getName))
                .containsExactlyInAnyOrder(metas.get(0).fileName(), metas.get(1).fileName());

        for (boolean missing : new boolean[] {false, true}) {
            for (ManifestFileMeta manifest : metas) {
                Path sidecar =
                        new Path(
                                tempDir.toString(),
                                "manifest/" + ManifestSidecar.fileName(manifest));
                if (missing) {
                    fileIO.delete(sidecar, false);
                } else {
                    fileIO.overwriteFileUtf8(sidecar, "corrupt sidecar");
                }
            }
            List<ManifestEntry> fallback = new ArrayList<>();
            scan.readManifestEntries(metas, false).forEachRemaining(fallback::add);
            assertThat(fallback).containsExactly(live);
        }
    }

    @Test
    void testBucketOnlyPlanningAndRawRewriteUseNullableBucketPayload() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 4);
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        RecordingFileIO io = new RecordingFileIO();
        ManifestFile.Factory factory =
                createManifestFileFactory(tempDir.toString(), Long.MAX_VALUE, options, io);
        ManifestFile manifests = factory.create();
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            ManifestEntry entry = gen.next();
            entries.add(
                    ManifestEntry.create(
                            FileKind.ADD, entry.partition(), i / 1000, 4, entry.file()));
        }
        ManifestFileMeta meta = manifests.write(entries).get(0);
        AppendOnlyFileStoreScan scan =
                new AppendOnlyFileStoreScan(
                        mock(ManifestsReader.class),
                        null,
                        null,
                        null,
                        mock(TableSchema.class),
                        factory,
                        2,
                        false,
                        false,
                        false);
        scan.withBucket(1);
        io.reset();
        assertThat(scan.readManifest(meta)).containsExactlyElementsOf(entries.subList(1000, 2000));
        assertThat(io.bytes.get()).isLessThan(meta.fileSize());
        assertThat(io.seeks).isNotEmpty();
        ManifestAvroWriter writer = manifests.createAvroWriter();
        try (ManifestAvroReader reader =
                manifests.scanAvroBlocks(meta.fileName(), meta.fileSize())) {
            writer.writeEncodedManifest(reader, meta);
        }
        writer.close();
        assertThat(scan.readManifest(writer.result().get(0)))
                .containsExactlyElementsOf(entries.subList(1000, 2000));

        ManifestEntry added = entries.get(1000);
        ManifestEntry deleted =
                ManifestEntry.create(FileKind.DELETE, added.partition(), 1, 4, added.file());
        List<ManifestFileMeta> changes = new ArrayList<>();
        changes.addAll(manifests.write(Collections.singletonList(added)));
        changes.addAll(manifests.write(Collections.singletonList(deleted)));
        assertThat(scan.readManifestEntries(changes, false)).isExhausted();
    }

    @Test
    void testPartitionOnlyPlanningUsesBlocksWithoutRowIds() {
        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        RecordingFileIO io = new RecordingFileIO();
        ManifestFile.Factory factory =
                createManifestFileFactory(tempDir.toString(), Long.MAX_VALUE, options, io);
        BinaryRow first = gen.next().partition();
        BinaryRow second = gen.next().partition();
        while (second.equals(first)) {
            second = gen.next().partition();
        }
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            ManifestEntry entry = gen.next();
            entries.add(
                    ManifestEntry.create(
                            FileKind.ADD,
                            i < 1000 ? first : second,
                            entry.bucket(),
                            entry.totalBuckets(),
                            entry.file()));
        }
        ManifestFileMeta meta = factory.create().write(entries).get(0);
        ManifestsReader lists = mock(ManifestsReader.class);
        when(lists.partitionFilter())
                .thenReturn(
                        PartitionPredicate.fromMultiple(
                                DEFAULT_PART_TYPE, Collections.singletonList(first)));
        AppendOnlyFileStoreScan scan =
                new AppendOnlyFileStoreScan(
                        lists,
                        null,
                        null,
                        null,
                        mock(TableSchema.class),
                        factory,
                        2,
                        false,
                        false,
                        false);
        io.reset();
        List<ManifestEntry> actual = scan.readManifest(meta);
        assertThat(actual).containsExactlyElementsOf(entries.subList(0, 1000));
        assertThat(io.bytes.get()).isLessThan(meta.fileSize());
        assertThat(io.opened).hasSize(2);
    }

    @Test
    void testUnfilteredReadWithoutCacheSkipsSidecar() {
        Options options = new Options();
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        RecordingFileIO fileIO = new RecordingFileIO();
        ManifestFile manifests =
                createManifestFileFactory(tempDir.toString(), Long.MAX_VALUE, options, fileIO)
                        .create();
        ManifestEntry entry = gen.next();
        ManifestFileMeta meta = manifests.write(Collections.singletonList(entry)).get(0);
        assertThat(ManifestSidecar.fileName(meta)).isNotNull();
        assertThat(
                        java.nio.file.Files.exists(
                                tempDir.resolve("manifest")
                                        .resolve(meta.fileName() + ManifestSidecar.SUFFIX)))
                .isTrue();

        fileIO.reset();
        assertThat(manifests.selectBlocks(meta, null)).isNull();
        assertThat(manifests.read(meta.fileName())).containsExactly(entry);
        assertThat(fileIO.opened)
                .containsExactly(new Path(tempDir.toString(), "manifest/" + meta.fileName()));
    }

    @Test
    void testUnfilteredReadWithCacheAndWithoutSidecarUsesWholeManifestCache() {
        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        RecordingFileIO io = new RecordingFileIO();
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(16), Long.MAX_VALUE);
        ManifestFile manifests =
                createManifestFileFactory(tempDir.toString(), Long.MAX_VALUE, options, io, cache)
                        .create();
        ManifestEntry entry = gen.next();
        ManifestFileMeta written = manifests.write(Collections.singletonList(entry)).get(0);
        ManifestFileMeta unindexed = withExtraFiles(written, null);
        Path manifestPath = new Path(tempDir.toString(), "manifest/" + written.fileName());

        io.reset();
        ManifestSidecar.Selection selected = manifests.selectBlocks(unindexed, null);
        assertThat(selected).isNull();
        assertThat(
                        manifests.read(
                                unindexed.fileName(),
                                unindexed.fileSize(),
                                null,
                                null,
                                row -> true,
                                manifestEntry -> true,
                                java.util.function.Function.identity(),
                                selected))
                .containsExactly(entry);
        assertThat(io.opened).containsExactly(manifestPath);
        assertThat(cache.getIfPresents(manifestPath)).isNotNull();

        io.reset();
        assertThat(manifests.read(unindexed.fileName())).containsExactly(entry);
        assertThat(io.opened).isEmpty();
    }

    @Test
    void testUnfilteredReadWarmsBlockCacheForFilteredRead() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 4);
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        RecordingFileIO io = new RecordingFileIO();
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(16), Long.MAX_VALUE);
        ManifestFile.Factory factory =
                createManifestFileFactory(tempDir.toString(), Long.MAX_VALUE, options, io, cache);
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            ManifestEntry entry = gen.next();
            entries.add(
                    ManifestEntry.create(
                            FileKind.ADD, entry.partition(), i / 1000, 4, entry.file()));
        }
        ManifestFileMeta meta = factory.create().write(entries).get(0);
        CacheMetrics metrics = new CacheMetrics();
        ManifestFile manifests = factory.create().withCacheMetrics(metrics);
        Path manifestPath = new Path(tempDir.toString(), "manifest/" + meta.fileName());
        Path sidecarPath = ManifestSidecar.path(manifestPath);

        io.reset();
        ManifestSidecar.Selection allBlocks = manifests.selectBlocks(meta, null);
        assertThat(readSelectedEntries(manifests, meta, allBlocks))
                .containsExactlyElementsOf(entries);
        assertThat(io.opened).containsExactly(sidecarPath, manifestPath);
        assertThat(cache.getIfPresents(manifestPath)).isNull();
        assertThat(metrics.getMissedObject()).hasValue(1);
        assertThat(metrics.getHitObject()).hasValue(0);

        io.reset();
        ManifestSidecar.Selection cachedBlocks = manifests.selectBlocks(meta, null);
        assertThat(readSelectedEntries(manifests, meta, cachedBlocks))
                .containsExactlyElementsOf(entries);
        assertThat(io.opened).isEmpty();
        assertThat(metrics.getMissedObject()).hasValue(1);
        assertThat(metrics.getHitObject()).hasValue(1);

        BucketFilter bucketFilter = new BucketFilter(false, 1, null, null);
        io.reset();
        ManifestSidecar.Selection selected = manifests.selectBlocks(meta, null, null, bucketFilter);
        assertThat(
                        manifests.read(
                                meta.fileName(),
                                meta.fileSize(),
                                null,
                                bucketFilter,
                                row -> true,
                                entry -> true,
                                java.util.function.Function.identity(),
                                selected))
                .containsExactlyElementsOf(entries.subList(1000, 2000));
        assertThat(io.opened).isEmpty();
        assertThat(io.bytes.get()).isZero();
        assertThat(metrics.getMissedObject()).hasValue(1);
        assertThat(metrics.getHitObject()).hasValue(2);
    }

    @Test
    void testExplicitIndexReferenceAndNullDoesNotProbe() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
        RecordingFileIO io = new RecordingFileIO();
        ManifestFile manifests =
                createManifestFileFactory(tempDir.toString(), Long.MAX_VALUE, options, io).create();
        ManifestEntry original = gen.next();
        ManifestEntry entry =
                ManifestEntry.create(
                        FileKind.ADD,
                        original.partition(),
                        original.bucket(),
                        original.totalBuckets(),
                        original.file().newFirstRowId(100L));
        ManifestFileMeta written = manifests.write(Collections.singletonList(entry)).get(0);
        assertThat(ManifestSidecar.fileName(written)).isNotNull();
        RowRangeIndex query = RowRangeIndex.create(Collections.singletonList(new Range(0, 0)));
        io.reset();
        for (List<String> extraFiles :
                Arrays.asList(
                        null,
                        Collections.<String>emptyList(),
                        Collections.singletonList("other-partition-index"))) {
            ManifestFileMeta unindexed = withExtraFiles(written, extraFiles);
            assertThat(manifests.selectBlocks(unindexed, query)).isNull();
            assertThat(io.opened).isEmpty();
        }
        // An existing suffix-named object must not be inferred as a reference.
        assertThat(
                        java.nio.file.Files.exists(
                                tempDir.resolve("manifest")
                                        .resolve(ManifestSidecar.fileName(written))))
                .isTrue();
        String explicitName = "custom-index-name" + ManifestSidecar.SUFFIX;
        java.nio.file.Files.move(
                tempDir.resolve("manifest").resolve(ManifestSidecar.fileName(written)),
                tempDir.resolve("manifest").resolve(explicitName));
        String otherName = "other-partition-index";
        java.nio.file.Path otherPath = tempDir.resolve("manifest").resolve(otherName);
        java.nio.file.Files.write(otherPath, new byte[] {1, 2, 3});
        ManifestFileMeta indexed = withExtraFiles(written, Arrays.asList(otherName, explicitName));
        assertThat(manifests.selectBlocks(indexed, query).blocks()).isEmpty();
        assertThat(io.opened)
                .containsExactly(new Path(tempDir.toString(), "manifest/" + explicitName));
        manifests.delete(indexed);
        assertThat(java.nio.file.Files.exists(otherPath)).isFalse();
        assertThat(java.nio.file.Files.exists(tempDir.resolve("manifest").resolve(explicitName)))
                .isFalse();
        assertThat(
                        java.nio.file.Files.exists(
                                tempDir.resolve("manifest").resolve(written.fileName())))
                .isFalse();
    }

    @Test
    void testDisabledOrUnfilteredReadsSkipSidecarMetadata() {
        for (boolean enabled : new boolean[] {false, true}) {
            Options options = new Options();
            options.set(CoreOptions.MANIFEST_SORT_ENABLED, true);
            options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, enabled);
            RecordingFileIO io = new RecordingFileIO();
            ManifestFile manifests =
                    createManifestFileFactory(tempDir.toString(), Long.MAX_VALUE, options, io)
                            .create();
            ManifestFileMeta meta = mock(ManifestFileMeta.class);
            RowRangeIndex rows =
                    enabled
                            ? null
                            : RowRangeIndex.create(Collections.singletonList(new Range(1, 1)));
            assertThat(manifests.selectBlocks(meta, rows)).isNull();
            verifyNoInteractions(meta);
            assertThat(io.opened).isEmpty();
        }
    }

    private List<ManifestEntry> readSelectedEntries(
            ManifestFile manifests, ManifestFileMeta meta, ManifestSidecar.Selection selected) {
        return manifests.read(
                meta.fileName(),
                meta.fileSize(),
                null,
                null,
                row -> true,
                entry -> true,
                java.util.function.Function.identity(),
                selected);
    }

    /** Observes actual file access without adding counters to production readers. */
    private static final class RecordingFileIO extends LocalFileIO {

        private final List<Path> opened = Collections.synchronizedList(new ArrayList<>());
        private final List<Long> seeks = Collections.synchronizedList(new ArrayList<>());
        private final AtomicLong bytes = new AtomicLong();

        private void reset() {
            opened.clear();
            seeks.clear();
            bytes.set(0);
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            opened.add(path);
            return new SeekableInputStreamWrapper(super.newInputStream(path)) {
                @Override
                public void seek(long desired) throws IOException {
                    seeks.add(desired);
                    super.seek(desired);
                }

                @Override
                public int read() throws IOException {
                    int value = super.read();
                    if (value >= 0) {
                        bytes.incrementAndGet();
                    }
                    return value;
                }

                @Override
                public int read(byte[] buffer, int offset, int length) throws IOException {
                    int n = super.read(buffer, offset, length);
                    if (n > 0) {
                        bytes.addAndGet(n);
                    }
                    return n;
                }
            };
        }
    }

    private ManifestFile createManifestFile(String pathStr) {
        return createManifestFile(pathStr, ThreadLocalRandom.current().nextInt(8192) + 1024);
    }

    private ManifestFile createManifestFile(String pathStr, long suggestedFileSize) {
        return createManifestFile(pathStr, suggestedFileSize, new Options());
    }

    private ManifestFile createManifestFile(
            String pathStr, long suggestedFileSize, @Nullable SegmentsCache<Path> cache) {
        return createManifestFileFactory(
                        pathStr,
                        suggestedFileSize,
                        new Options(),
                        FileIOFinder.find(new Path(pathStr)),
                        cache)
                .create();
    }

    private ManifestFile createManifestFile(
            String pathStr, long suggestedFileSize, Options options) {
        return createManifestFileFactory(
                        pathStr, suggestedFileSize, options, FileIOFinder.find(new Path(pathStr)))
                .create();
    }

    private ManifestFile.Factory createManifestFileFactory(
            String pathStr, long suggestedFileSize, Options options, FileIO fileIO) {
        return createManifestFileFactory(pathStr, suggestedFileSize, options, fileIO, null);
    }

    private ManifestFile.Factory createManifestFileFactory(
            String pathStr,
            long suggestedFileSize,
            Options options,
            FileIO fileIO,
            @Nullable SegmentsCache<Path> cache) {
        return createManifestFileFactory(pathStr, suggestedFileSize, options, fileIO, cache, null);
    }

    private ManifestFile.Factory createManifestFileFactory(
            String pathStr,
            long suggestedFileSize,
            Options options,
            FileIO fileIO,
            @Nullable SegmentsCache<Path> cache,
            @Nullable SegmentsCache<Path> sidecarCache) {
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
        CoreOptions coreOptions = new CoreOptions(options);
        return new ManifestFile.Factory(
                fileIO,
                new FileSystemSchemaManager(fileIO, path),
                DEFAULT_PART_TYPE,
                avro,
                "zstd",
                pathFactory,
                suggestedFileSize,
                cache,
                sidecarCache,
                coreOptions);
    }

    @Test
    void testBucketFilterPushedDownWhenManifestExceedsCacheElementSize() throws Exception {
        List<ManifestEntry> entries = generateData();
        Set<Integer> buckets =
                entries.stream().map(ManifestEntry::bucket).collect(Collectors.toSet());
        assertThat(buckets.size()).isGreaterThan(1);

        // A manifest above the cache element size limit is read uncached; the bucket filter must
        // still reach the Avro reader instead of being applied after decoding every entry.
        SegmentsCache<Path> tinyElementCache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(8), 1L);
        ManifestFile manifestFile =
                createManifestFile(tempDir.toString(), Long.MAX_VALUE, tinyElementCache);
        List<ManifestFileMeta> metas = manifestFile.write(entries);
        assertThat(metas).hasSize(1);
        ManifestFileMeta meta = metas.get(0);

        for (int bucket : buckets) {
            BucketFilter bucketFilter = BucketFilter.create(false, bucket, null, null);
            List<ManifestEntry> actual =
                    manifestFile.read(
                            meta.fileName(),
                            meta.fileSize(),
                            null,
                            bucketFilter,
                            Filter.alwaysTrue(),
                            Filter.alwaysTrue());
            List<ManifestEntry> expected =
                    entries.stream()
                            .filter(entry -> entry.bucket() == bucket)
                            .collect(Collectors.toList());
            assertThat(actual).isEqualTo(expected);
        }

        // Without any pushdown filter every entry must still be returned.
        assertThat(
                        manifestFile.read(
                                meta.fileName(),
                                meta.fileSize(),
                                null,
                                null,
                                Filter.alwaysTrue(),
                                Filter.alwaysTrue()))
                .isEqualTo(entries);
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
