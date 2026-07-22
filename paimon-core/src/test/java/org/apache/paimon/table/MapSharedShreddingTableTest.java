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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.shredding.MapSharedShreddingFieldMeta;
import org.apache.paimon.data.shredding.MapSharedShreddingUtils;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.SupportsFieldMetadata;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.assertj.core.api.Assertions.assertThat;

/** Table-level tests for MAP shared-shredding. */
public class MapSharedShreddingTableTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testAppendOnlyTableReadWrite(String format) throws Exception {
        Table table = createTable(format, "metrics");

        write(
                table,
                GenericRow.of(1, mapOf("a", 11L, "b", 12L, "c", 13L)),
                GenericRow.of(2, mapOf()),
                GenericRow.of(3, null),
                GenericRow.of(4, mapOf("a", null, "b", 42L, "c", null)));

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }

        assertThat(actual)
                .containsEntry(1, javaMapOf("a", 11L, "b", 12L, "c", 13L))
                .containsEntry(2, javaMapOf())
                .containsEntry(3, null)
                .containsEntry(4, javaMapOf("a", null, "b", 42L, "c", null));
    }

    @ParameterizedTest
    @CsvSource({
        "orc,plain",
        "orc,sequential",
        "orc,lru",
        "parquet,plain",
        "parquet,sequential",
        "parquet,lru"
    })
    public void testColumnPlacementPolicies(String format, String placementPolicy)
            throws Exception {
        Table table = createTable(format, 3, "metrics");
        catalog.alterTable(
                identifier(format),
                Collections.singletonList(
                        SchemaChange.setOption(
                                "fields.metrics.map.shared-shredding.column-placement-policy",
                                placementPolicy)),
                false);
        table = catalog.getTable(identifier(format));

        write(
                table,
                GenericRow.of(1, mapOf("a", 10L, "b", 20L, "c", 30L)),
                GenericRow.of(2, mapOf("a", 40L, "b", 50L)),
                GenericRow.of(3, mapOf("d", 60L)),
                GenericRow.of(4, mapOf("a", 70L, "b", 80L, "c", 90L, "d", 100L)));

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileWithSplit> files = currentDataFiles(fileStoreTable);
        assertThat(files).hasSize(1);
        MapSharedShreddingFieldMeta fieldMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(0), "metrics");
        assertThat(fieldMeta.nameToId()).containsOnlyKeys("a", "b", "c", "d");
        assertThat(fieldMeta.numColumns()).isEqualTo(3);
        assertThat(fieldMeta.maxRowWidth()).isEqualTo(4);
        assertThat(fieldMeta.overflowFieldSet()).hasSize(1);

        if ("lru".equals(placementPolicy)) {
            assertThat(fieldMeta.overflowFieldSet()).containsExactly(fieldMeta.nameToId().get("c"));
        } else {
            assertThat(fieldMeta.overflowFieldSet()).containsExactly(fieldMeta.nameToId().get("d"));
        }

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), toJavaMap(row.getMap(1)));
        }
        assertThat(actual)
                .containsOnlyKeys(1, 2, 3, 4)
                .containsEntry(1, javaMapOf("a", 10L, "b", 20L, "c", 30L))
                .containsEntry(2, javaMapOf("a", 40L, "b", 50L))
                .containsEntry(3, javaMapOf("d", 60L))
                .containsEntry(4, javaMapOf("a", 70L, "b", 80L, "c", 90L, "d", 100L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testAppendOnlyTableReadWriteWithTwoMapFields(String format) throws Exception {
        Table table = createTable(format, "metrics", "labels");

        write(
                table,
                GenericRow.of(
                        1,
                        mapOf("a", 11L, "b", 12L, "c", 13L),
                        mapOf("x", 21L, "y", 22L, "z", 23L)),
                GenericRow.of(2, mapOf("a", 31L), mapOf()),
                GenericRow.of(3, null, mapOf("x", 41L)));

        Map<Integer, List<Map<String, Long>>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(
                    row.getInt(0),
                    Arrays.asList(
                            row.isNullAt(1) ? null : toJavaMap(row.getMap(1)),
                            row.isNullAt(2) ? null : toJavaMap(row.getMap(2))));
        }

        assertThat(actual)
                .containsEntry(
                        1,
                        Arrays.asList(
                                javaMapOf("a", 11L, "b", 12L, "c", 13L),
                                javaMapOf("x", 21L, "y", 22L, "z", 23L)))
                .containsEntry(2, Arrays.asList(javaMapOf("a", 31L), javaMapOf()))
                .containsEntry(3, Arrays.asList(null, javaMapOf("x", 41L)));

        Map<Integer, Map<String, Long>> projected = new LinkedHashMap<>();
        for (InternalRow row : read(table, new int[] {0, 2})) {
            projected.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }

        assertThat(projected)
                .containsEntry(1, javaMapOf("x", 21L, "y", 22L, "z", 23L))
                .containsEntry(2, javaMapOf())
                .containsEntry(3, javaMapOf("x", 41L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testAppendOnlyCompaction(String format) throws Exception {
        Table table = createCompactingAppendOnlyTable(format);

        write(table, GenericRow.of(1, mapOf("a", 11L, "b", 12L)));
        write(table, GenericRow.of(2, mapOf("c", 21L)));
        write(table, GenericRow.of(3, mapOf("d", 31L, "e", 32L, "f", 33L, "g", 34L)));
        compact(table, BinaryRow.EMPTY_ROW, 0);

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileWithSplit> files = currentDataFiles(fileStoreTable);
        assertThat(files).hasSize(1);
        assertThat(files.get(0).dataFile.fileSource()).hasValue(FileSource.COMPACT);
        MapSharedShreddingFieldMeta compactedMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(0), "metrics");
        assertThat(compactedMeta.numColumns()).isEqualTo(2);
        assertThat(compactedMeta.maxRowWidth()).isEqualTo(4);
        assertThat(compactedMeta.nameToId()).containsOnlyKeys("a", "b", "c", "d", "e", "f", "g");

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), toJavaMap(row.getMap(1)));
        }
        assertThat(actual)
                .containsOnlyKeys(1, 2, 3)
                .containsEntry(1, javaMapOf("a", 11L, "b", 12L))
                .containsEntry(2, javaMapOf("c", 21L))
                .containsEntry(3, javaMapOf("d", 31L, "e", 32L, "f", 33L, "g", 34L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testAppendOnlyTableReadWriteWithComplexValue(String format) throws Exception {
        Table table = createComplexValueTable(format);

        write(
                table,
                GenericRow.of(
                        1,
                        complexMapOf(
                                "a",
                                complexValue(11L, stringArray("x", "y"), longMapOf("p", 101L)),
                                "b",
                                complexValue(12L, stringArray("z"), longMapOf("q", 102L)),
                                "c",
                                complexValue(13L, null, longMapOf("r", null)))),
                GenericRow.of(
                        2,
                        complexMapOf(
                                "single",
                                complexValue(
                                        null,
                                        stringArray("single-tag"),
                                        longMapOf("k1", 201L, "k2", 202L)))));

        Map<Integer, Map<String, ComplexValue>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), toJavaComplexMap(row.getMap(1)));
        }

        assertThat(actual)
                .containsEntry(
                        1,
                        javaComplexMapOf(
                                "a",
                                new ComplexValue(
                                        11L, Arrays.asList("x", "y"), javaMapOf("p", 101L)),
                                "b",
                                new ComplexValue(
                                        12L, Collections.singletonList("z"), javaMapOf("q", 102L)),
                                "c",
                                new ComplexValue(13L, null, javaMapOf("r", null))))
                .containsEntry(
                        2,
                        javaComplexMapOf(
                                "single",
                                new ComplexValue(
                                        null,
                                        Collections.singletonList("single-tag"),
                                        javaMapOf("k1", 201L, "k2", 202L))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testAppendOnlyTableReadWriteWithAllSupportedComplexValueTypes(String format)
            throws Exception {
        Table table = createAllSupportedValueTypesTable(format);

        WideValue fixedValue =
                new WideValue(
                        true,
                        (byte) 1,
                        (short) 2,
                        3,
                        4L,
                        5.5f,
                        6.25d,
                        "str",
                        "varchar",
                        "char",
                        new byte[] {1, 2, 3},
                        new byte[] {4, 5},
                        Decimal.fromBigDecimal(new BigDecimal("12345678.90"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("123456789012345678.12345"), 23, 5),
                        19500,
                        12_345,
                        Timestamp.fromEpochMillis(1_700_000_000_123L),
                        Timestamp.fromEpochMillis(1_700_000_000_123L, 456_789),
                        Timestamp.fromEpochMillis(1_700_000_000_123L),
                        Timestamp.fromEpochMillis(1_700_000_000_123L, 456_000),
                        Arrays.asList(7, null, 8),
                        javaMapOf("m1", 10L, "m2", null),
                        new NestedValue("nested", 99));
        WideValue sparseValue =
                new WideValue(
                        null, null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null, null);
        WideValue overflowValue =
                new WideValue(
                        false,
                        (byte) 9,
                        (short) 10,
                        11,
                        12L,
                        13.5f,
                        14.25d,
                        "overflow",
                        "overflow-varchar",
                        "over",
                        new byte[] {9, 8, 7},
                        new byte[] {6, 5, 4, 3},
                        Decimal.fromBigDecimal(new BigDecimal("-1.23"), 10, 2),
                        Decimal.fromBigDecimal(new BigDecimal("-123456789012345678.12345"), 23, 5),
                        19600,
                        54_321,
                        Timestamp.fromEpochMillis(1_800_000_000_321L),
                        Timestamp.fromEpochMillis(1_800_000_000_321L, 987_654),
                        Timestamp.fromEpochMillis(1_800_000_000_321L),
                        Timestamp.fromEpochMillis(1_800_000_000_321L, 987_000),
                        Collections.singletonList(42),
                        javaMapOf("om", 100L),
                        new NestedValue("overflow-nested", -7));

        write(
                table,
                GenericRow.of(
                        1,
                        wideMapOf(
                                "fixed-a",
                                toWideRow(fixedValue),
                                "fixed-b",
                                toWideRow(sparseValue),
                                "overflow",
                                toWideRow(overflowValue))),
                GenericRow.of(2, null),
                GenericRow.of(3, wideMapOf()));

        Map<Integer, Map<String, WideValue>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.isNullAt(1) ? null : toJavaWideMap(row.getMap(1)));
        }

        assertThat(actual)
                .containsEntry(
                        1,
                        javaWideMapOf(
                                "fixed-a",
                                fixedValue,
                                "fixed-b",
                                sparseValue,
                                "overflow",
                                overflowValue))
                .containsEntry(2, null)
                .containsEntry(3, Collections.emptyMap());
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testInferColumnCountFromFirstRowOfEachFile(String format) throws Exception {
        Table table = createTableWithBucket(format, 8, "1", "metrics");

        write(
                table,
                GenericRow.of(1, mapOf("a", 11L, "b", 12L)),
                GenericRow.of(2, mapOf("c", 21L, "d", 22L, "e", 23L)));
        write(table, GenericRow.of(3, mapOf("f", 31L)));

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileWithSplit> files = currentDataFiles(fileStoreTable);
        files.sort(Comparator.comparingLong(file -> file.dataFile.minSequenceNumber()));
        assertThat(files).hasSize(2);

        MapSharedShreddingFieldMeta firstFileMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(0), "metrics");
        assertThat(firstFileMeta.numColumns()).isEqualTo(2);
        assertThat(firstFileMeta.maxRowWidth()).isEqualTo(3);

        MapSharedShreddingFieldMeta secondFileMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(1), "metrics");
        assertThat(secondFileMeta.numColumns()).isEqualTo(1);
        assertThat(secondFileMeta.maxRowWidth()).isEqualTo(1);

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }

        assertThat(actual)
                .containsEntry(1, javaMapOf("a", 11L, "b", 12L))
                .containsEntry(2, javaMapOf("c", 21L, "d", 22L, "e", 23L))
                .containsEntry(3, javaMapOf("f", 31L));
    }

    @ParameterizedTest
    @CsvSource({"orc,false", "orc,true", "parquet,false", "parquet,true"})
    public void testPrimaryKeyWriteOnlyReadWrite(String format, boolean thinMode) throws Exception {
        Table table = createPrimaryKeyTable(format, thinMode, 2);

        write(
                table,
                GenericRow.of(1, mapOf("a", 11L, "b", 12L, "c", 13L)),
                GenericRow.of(2, null),
                GenericRow.of(3, mapOf()));
        write(
                table,
                GenericRow.of(1, mapOf("x", null, "y", 42L, "z", null)),
                GenericRow.of(4, mapOf("d", 44L)));

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }

        assertThat(actual)
                .containsOnlyKeys(1, 2, 3, 4)
                .containsEntry(1, javaMapOf("x", null, "y", 42L, "z", null))
                .containsEntry(2, null)
                .containsEntry(3, javaMapOf())
                .containsEntry(4, javaMapOf("d", 44L));
    }

    @ParameterizedTest
    @CsvSource({"orc,parquet", "parquet,orc"})
    public void testPrimaryKeyInputChangelog(String dataFileFormat, String changelogFileFormat)
            throws Exception {
        Table table = createPrimaryKeyInputChangelogTable(dataFileFormat, changelogFileFormat);

        List<CommitMessage> messages =
                writeAndCommit(
                        table,
                        GenericRow.ofKind(RowKind.INSERT, 1, mapOf("a", 11L, "b", 12L)),
                        GenericRow.ofKind(RowKind.INSERT, 2, mapOf("c", 21L)),
                        GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1, mapOf("a", 11L, "b", 12L)),
                        GenericRow.ofKind(
                                RowKind.UPDATE_AFTER,
                                1,
                                mapOf("x", 31L, "y", 32L, "overflow", 33L)),
                        GenericRow.ofKind(RowKind.DELETE, 2, mapOf("c", 21L)));

        List<DataFileWithSplit> changelogFiles = new ArrayList<>();
        for (CommitMessage message : messages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            for (DataFileMeta file : commitMessage.newFilesIncrement().changelogFiles()) {
                changelogFiles.add(
                        new DataFileWithSplit(
                                commitMessage.partition(), commitMessage.bucket(), file));
            }
        }
        assertThat(changelogFiles).hasSize(1);
        DataFileWithSplit changelogFile = changelogFiles.get(0);
        assertThat(changelogFile.dataFile.fileFormat()).isEqualTo(changelogFileFormat);
        assertThat(changelogFile.dataFile.fileName())
                .startsWith(((FileStoreTable) table).coreOptions().changelogFilePrefix());

        MapSharedShreddingFieldMeta changelogMeta =
                readSharedShreddingFieldMeta((FileStoreTable) table, changelogFile, "metrics");
        assertThat(changelogMeta.numColumns()).isEqualTo(2);
        assertThat(changelogMeta.maxRowWidth()).isEqualTo(3);
        assertThat(changelogMeta.nameToId()).containsOnlyKeys("a", "b", "c", "x", "y", "overflow");

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<Split> changelogSplits =
                fileStoreTable.newSnapshotReader().withMode(ScanMode.CHANGELOG).read().splits();
        Map<String, Map<String, Long>> actualChangelog = new LinkedHashMap<>();
        try (RecordReader<InternalRow> reader =
                fileStoreTable.newRead().createReader(changelogSplits)) {
            reader.forEachRemaining(
                    row ->
                            actualChangelog.put(
                                    row.getRowKind().shortString() + ":" + row.getInt(0),
                                    toJavaMap(row.getMap(1))));
        }
        assertThat(actualChangelog)
                .containsEntry("+I:1", javaMapOf("a", 11L, "b", 12L))
                .containsEntry("+I:2", javaMapOf("c", 21L))
                .containsEntry("-U:1", javaMapOf("a", 11L, "b", 12L))
                .containsEntry("+U:1", javaMapOf("x", 31L, "y", 32L, "overflow", 33L))
                .containsEntry("-D:2", javaMapOf("c", 21L));

        assertThat(readMapsById(table.newReadBuilder()))
                .containsOnlyKeys(1)
                .containsEntry(1, javaMapOf("x", 31L, "y", 32L, "overflow", 33L));
    }

    @ParameterizedTest
    @CsvSource({"orc,false", "orc,true", "parquet,false", "parquet,true"})
    public void testPrimaryKeyMergeMapAggregation(String format, boolean thinMode)
            throws Exception {
        Table table = createPrimaryKeyAggregationTable(format, thinMode);

        write(table, GenericRow.of(1, mapOf("a", 11L, "b", 12L)), GenericRow.of(2, null));
        write(
                table,
                GenericRow.of(1, mapOf("b", 22L, "c", 23L)),
                GenericRow.of(2, mapOf("x", 31L)));

        assertThat(readMapsById(table.newReadBuilder()))
                .containsOnlyKeys(1, 2)
                .containsEntry(1, javaMapOf("a", 11L, "b", 22L, "c", 23L))
                .containsEntry(2, javaMapOf("x", 31L));

        compact(table, BinaryRow.EMPTY_ROW, 0);

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileWithSplit> files = currentDataFiles(fileStoreTable);
        assertThat(files).hasSize(1);
        assertThat(files.get(0).dataFile.fileSource()).hasValue(FileSource.COMPACT);
        assertThat(readMapsById(table.newReadBuilder()))
                .containsOnlyKeys(1, 2)
                .containsEntry(1, javaMapOf("a", 11L, "b", 22L, "c", 23L))
                .containsEntry(2, javaMapOf("x", 31L));
    }

    @ParameterizedTest
    @CsvSource({"orc,false", "orc,true", "parquet,false", "parquet,true"})
    public void testPrimaryKeyExternalSpillReadWrite(String format, boolean thinMode)
            throws Exception {
        Table table = createPrimaryKeyTable(format, thinMode, 2);
        Map<String, String> spillOptions = new LinkedHashMap<>();
        spillOptions.put(CoreOptions.PAGE_SIZE.key(), "4 kb");
        spillOptions.put(CoreOptions.WRITE_BUFFER_SIZE.key(), "12 kb");
        spillOptions.put(CoreOptions.WRITE_BUFFER_SPILLABLE.key(), "true");
        table = table.copy(spillOptions);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            for (int i = 0; i < 1000; i++) {
                write.write(
                        GenericRow.of(
                                i % 100,
                                mapOf("a", (long) i, "b", (long) i + 1, "overflow", (long) -i)));
            }
            assertThat(numberOfSpillFiles()).isGreaterThan(0);
            commit.commit(write.prepareCommit());
        }

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), toJavaMap(row.getMap(1)));
        }

        assertThat(actual).hasSize(100);
        for (int id = 0; id < 100; id++) {
            long lastValue = 900L + id;
            assertThat(actual)
                    .containsEntry(
                            id,
                            javaMapOf("a", lastValue, "b", lastValue + 1, "overflow", -lastValue));
        }
    }

    @ParameterizedTest
    @CsvSource({"orc,false", "orc,true", "parquet,false", "parquet,true"})
    public void testPrimaryKeyCompaction(String format, boolean thinMode) throws Exception {
        Table table = createPrimaryKeyTable(format, thinMode, 2, false);

        write(
                table,
                GenericRow.of(1, mapOf("old", 11L)),
                GenericRow.of(2, mapOf("a", 21L, "b", 22L, "overflow", 23L)));
        write(
                table,
                GenericRow.of(1, mapOf("x", 31L, "y", 32L, "overflow", 33L)),
                GenericRow.of(3, null));
        compact(table, BinaryRow.EMPTY_ROW, 0);

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileWithSplit> files = currentDataFiles(fileStoreTable);
        assertThat(files).hasSize(1);
        assertThat(files.get(0).dataFile.fileSource()).hasValue(FileSource.COMPACT);
        MapSharedShreddingFieldMeta compactedMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(0), "metrics");
        assertThat(compactedMeta.numColumns()).isEqualTo(2);
        assertThat(compactedMeta.maxRowWidth()).isEqualTo(3);
        assertThat(compactedMeta.nameToId()).containsOnlyKeys("a", "b", "overflow", "x", "y");

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }
        assertThat(actual)
                .containsOnlyKeys(1, 2, 3)
                .containsEntry(1, javaMapOf("x", 31L, "y", 32L, "overflow", 33L))
                .containsEntry(2, javaMapOf("a", 21L, "b", 22L, "overflow", 23L))
                .containsEntry(3, null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testPrimaryKeyDeletionVectorCompaction(String format) throws Exception {
        Table table = createPrimaryKeyTable(format, false, 2, false, true);
        String padding = String.join("", Collections.nCopies(2048, "X"));

        write(
                table,
                ioManager,
                GenericRow.of(1, mapOf("a", 10L, "b", 20L), BinaryString.fromString(padding)),
                GenericRow.of(2, mapOf("c", 30L), BinaryString.fromString(padding)),
                GenericRow.of(3, mapOf("d", 40L), BinaryString.fromString(padding)),
                GenericRow.of(4, null, BinaryString.fromString(padding)),
                GenericRow.of(6, mapOf("j", 60L, "k", 70L), BinaryString.fromString(padding)),
                GenericRow.of(
                        7, mapOf("l", 80L, "m", 90L, "n", 100L), BinaryString.fromString(padding)),
                GenericRow.of(8, mapOf("o", 110L), BinaryString.fromString(padding)));
        List<CommitMessage> upgradeMessages = compactAndCommit(table, BinaryRow.EMPTY_ROW, 0, true);
        assertThat(upgradeMessages)
                .allSatisfy(
                        message ->
                                assertThat(
                                                ((CommitMessageImpl) message)
                                                        .compactIncrement()
                                                        .newIndexFiles())
                                        .isEmpty());

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileWithSplit> upgradedFiles = currentDataFiles(fileStoreTable);
        assertThat(upgradedFiles).hasSize(1);
        assertThat(upgradedFiles.get(0).dataFile.fileSize())
                .isGreaterThan(fileStoreTable.coreOptions().compactionFileSize(true));

        List<CommitMessage> compactMessages = new ArrayList<>();
        compactMessages.addAll(
                writeAndCommit(
                        table,
                        GenericRow.of(
                                1, mapOf("a", 100L, "e", 500L), BinaryString.fromString("u1")),
                        GenericRow.of(5, mapOf("h", 80L), BinaryString.fromString("u5"))));
        compactMessages.addAll(
                writeAndCommit(
                        table,
                        GenericRow.of(
                                2,
                                mapOf("c", 300L, "f", 600L, "g", 700L),
                                BinaryString.fromString("u2")),
                        GenericRow.of(
                                5,
                                mapOf("h", 800L, "i", 900L),
                                BinaryString.fromString("u5-new"))));
        compactMessages.addAll(compactAndCommit(table, BinaryRow.EMPTY_ROW, 0, false));
        List<IndexFileMeta> deletionVectorFiles = new ArrayList<>();
        for (CommitMessage message : compactMessages) {
            deletionVectorFiles.addAll(
                    ((CommitMessageImpl) message).compactIncrement().newIndexFiles());
        }
        assertThat(deletionVectorFiles)
                .isNotEmpty()
                .allSatisfy(file -> assertThat(file.indexType()).isEqualTo(DELETION_VECTORS_INDEX));
        assertThat(deletionVectorFiles)
                .anySatisfy(file -> assertThat(file.dvRanges()).isNotEmpty());

        Map<Integer, Map<String, Long>> actual = readMapsById(table.newReadBuilder());
        assertThat(actual)
                .containsOnlyKeys(1, 2, 3, 4, 5, 6, 7, 8)
                .containsEntry(1, javaMapOf("a", 100L, "e", 500L))
                .containsEntry(2, javaMapOf("c", 300L, "f", 600L, "g", 700L))
                .containsEntry(3, javaMapOf("d", 40L))
                .containsEntry(4, null)
                .containsEntry(5, javaMapOf("h", 800L, "i", 900L))
                .containsEntry(6, javaMapOf("j", 60L, "k", 70L))
                .containsEntry(7, javaMapOf("l", 80L, "m", 90L, "n", 100L))
                .containsEntry(8, javaMapOf("o", 110L));
    }

    @ParameterizedTest
    @CsvSource({"orc,false", "orc,true", "parquet,false", "parquet,true"})
    public void testPrimaryKeyInfersColumnCountPerFile(String format, boolean thinMode)
            throws Exception {
        Table table = createPrimaryKeyTable(format, thinMode, 8);

        write(table, GenericRow.of(1, mapOf("a", 11L, "b", 12L)));
        write(table, GenericRow.of(2, mapOf("c", 22L)));

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileWithSplit> files = currentDataFiles(fileStoreTable);
        files.sort(Comparator.comparingLong(file -> file.dataFile.minSequenceNumber()));
        assertThat(files).hasSize(2);

        MapSharedShreddingFieldMeta firstFileMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(0), "metrics");
        assertThat(firstFileMeta.numColumns()).isEqualTo(2);
        assertThat(firstFileMeta.maxRowWidth()).isEqualTo(2);

        MapSharedShreddingFieldMeta secondFileMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(1), "metrics");
        assertThat(secondFileMeta.numColumns()).isEqualTo(1);
        assertThat(secondFileMeta.maxRowWidth()).isEqualTo(1);

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), toJavaMap(row.getMap(1)));
        }
        assertThat(actual)
                .containsEntry(1, javaMapOf("a", 11L, "b", 12L))
                .containsEntry(2, javaMapOf("c", 22L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testSwitchMapLayoutAndInferColumns(String format) throws Exception {
        Table table =
                createTableWithBucket(
                        format,
                        4,
                        "1",
                        Arrays.asList("metrics", "labels"),
                        Arrays.asList("labels"));

        write(table, GenericRow.of(1, mapOf("a", 11L, "b", 12L), mapOf("x", 21L)));

        catalog.alterTable(
                identifier(format),
                Arrays.asList(
                        SchemaChange.setOption(
                                "fields.metrics.map.storage-layout", "shared-shredding"),
                        SchemaChange.setOption(
                                "fields.metrics.map.shared-shredding.max-columns", "3"),
                        SchemaChange.setOption("fields.labels.map.storage-layout", "default")),
                false);
        table = catalog.getTable(identifier(format));

        write(table, GenericRow.of(2, mapOf("c", 31L), mapOf("y", 41L, "z", 42L)));

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileWithSplit> files = currentDataFiles(fileStoreTable);
        files.sort(Comparator.comparingLong(file -> file.dataFile.minSequenceNumber()));
        assertThat(files).hasSize(2);

        MapSharedShreddingFieldMeta metricsMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(1), "metrics");
        assertThat(metricsMeta.numColumns()).isEqualTo(1);
        assertThat(metricsMeta.maxRowWidth()).isEqualTo(1);

        Map<Integer, List<Map<String, Long>>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(
                    row.getInt(0),
                    Arrays.asList(
                            row.isNullAt(1) ? null : toJavaMap(row.getMap(1)),
                            row.isNullAt(2) ? null : toJavaMap(row.getMap(2))));
        }

        assertThat(actual)
                .containsEntry(1, Arrays.asList(javaMapOf("a", 11L, "b", 12L), javaMapOf("x", 21L)))
                .containsEntry(
                        2, Arrays.asList(javaMapOf("c", 31L), javaMapOf("y", 41L, "z", 42L)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testReadSharedShreddingMapAfterRenameColumn(String format) throws Exception {
        Table table = createTable(format, "metrics");

        write(
                table,
                GenericRow.of(1, mapOf("a", 11L, "b", 12L)),
                GenericRow.of(2, mapOf("c", 21L)));

        catalog.alterTable(
                identifier(format),
                Arrays.asList(
                        SchemaChange.renameColumn("metrics", "renamed_metrics"),
                        SchemaChange.removeOption("fields.metrics.map.storage-layout"),
                        SchemaChange.removeOption(
                                "fields.metrics.map.shared-shredding.max-columns"),
                        SchemaChange.setOption(
                                "fields.renamed_metrics.map.storage-layout", "shared-shredding"),
                        SchemaChange.setOption(
                                "fields.renamed_metrics.map.shared-shredding.max-columns", "2")),
                false);
        table = catalog.getTable(identifier(format));

        assertThat(table.rowType().getFieldNames()).containsExactly("id", "renamed_metrics");

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }

        assertThat(actual)
                .containsEntry(1, javaMapOf("a", 11L, "b", 12L))
                .containsEntry(2, javaMapOf("c", 21L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testDataEvolutionMergeWithOverwrittenSharedShreddingMaps(String format)
            throws Exception {
        Table table = createDataEvolutionTable(format, "metrics", "labels");
        RowType rowType = table.rowType();

        writeWithWriteType(
                table,
                rowType.project(Arrays.asList("id", "metrics")),
                GenericRow.of(1, mapOf("old-a", 11L)),
                GenericRow.of(2, mapOf("old-b", 21L)));
        writeWithWriteType(
                table,
                rowType.project(Arrays.asList("id", "labels")),
                0L,
                GenericRow.of(101, mapOf("label-a", 101L)),
                GenericRow.of(102, mapOf("label-b", 102L)));
        writeWithWriteType(
                table,
                rowType.project(Collections.singletonList("metrics")),
                0L,
                GenericRow.of(mapOf("new-a", 111L)),
                GenericRow.of(mapOf("new-b", 222L)));

        Map<Integer, List<Map<String, Long>>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(
                    row.getInt(0),
                    Arrays.asList(toJavaMap(row.getMap(1)), toJavaMap(row.getMap(2))));
        }

        assertThat(actual)
                .containsEntry(
                        101, Arrays.asList(javaMapOf("new-a", 111L), javaMapOf("label-a", 101L)))
                .containsEntry(
                        102, Arrays.asList(javaMapOf("new-b", 222L), javaMapOf("label-b", 102L)));

        Map<Integer, List<Map<String, Long>>> partialActual = new LinkedHashMap<>();
        ReadBuilder readBuilder =
                table.newReadBuilder().withRowRanges(Collections.singletonList(new Range(1L, 1L)));
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row ->
                                partialActual.put(
                                        row.getInt(0),
                                        Arrays.asList(
                                                toJavaMap(row.getMap(1)),
                                                toJavaMap(row.getMap(2)))));

        assertThat(partialActual)
                .containsOnlyKeys(102)
                .containsEntry(
                        102, Arrays.asList(javaMapOf("new-b", 222L), javaMapOf("label-b", 102L)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testSharedShreddingWithBlobAndVector(String format) throws Exception {
        Table table = createBlobAndVectorTable(format);

        write(
                table,
                GenericRow.of(
                        1,
                        mapOf("a", 11L, "b", 12L, "overflow", 13L),
                        new BlobData(new byte[] {1, 2, 3}),
                        BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f})),
                GenericRow.of(2, null, null, null),
                GenericRow.of(
                        3,
                        mapOf(),
                        new BlobData(new byte[] {4, 5}),
                        BinaryVector.fromPrimitiveArray(new float[] {4.0f, 5.0f, 6.0f})));

        Map<Integer, Map<String, Long>> initialMaps = new LinkedHashMap<>();
        for (InternalRow row : read(table, new int[] {0, 1})) {
            initialMaps.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }
        assertThat(initialMaps)
                .containsEntry(1, javaMapOf("a", 11L, "b", 12L, "overflow", 13L))
                .containsEntry(2, null)
                .containsEntry(3, javaMapOf());

        RowType rowType = table.rowType();
        writeWithWriteType(
                table,
                rowType.project(Collections.singletonList("metrics")),
                0L,
                GenericRow.of(mapOf("updated", 101L)),
                GenericRow.of(mapOf("updated", 102L)),
                GenericRow.of(mapOf("updated", 103L)));

        Map<Integer, Map<String, Long>> maps = new LinkedHashMap<>();
        Map<Integer, byte[]> blobs = new LinkedHashMap<>();
        Map<Integer, float[]> vectors = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            int id = row.getInt(0);
            maps.put(id, toJavaMap(row.getMap(1)));
            blobs.put(id, row.isNullAt(2) ? null : row.getBlob(2).toData());
            vectors.put(id, row.isNullAt(3) ? null : row.getVector(3).toFloatArray());
        }

        assertThat(maps)
                .containsEntry(1, javaMapOf("updated", 101L))
                .containsEntry(2, javaMapOf("updated", 102L))
                .containsEntry(3, javaMapOf("updated", 103L));
        assertThat(blobs.get(1)).containsExactly(1, 2, 3);
        assertThat(blobs.get(2)).isNull();
        assertThat(blobs.get(3)).containsExactly(4, 5);
        assertThat(vectors.get(1)).containsExactly(1.0f, 2.0f, 3.0f);
        assertThat(vectors.get(2)).isNull();
        assertThat(vectors.get(3)).containsExactly(4.0f, 5.0f, 6.0f);

        Map<Integer, Map<String, Long>> projectedMaps = new LinkedHashMap<>();
        for (InternalRow row : read(table, new int[] {0, 1})) {
            projectedMaps.put(row.getInt(0), toJavaMap(row.getMap(1)));
        }
        assertThat(projectedMaps).isEqualTo(maps);

        for (InternalRow row : read(table, new int[] {0, 2, 3})) {
            int id = row.getInt(0);
            byte[] expectedBlob = blobs.get(id);
            if (expectedBlob == null) {
                assertThat(row.isNullAt(1)).isTrue();
            } else {
                assertThat(row.getBlob(1).toData()).containsExactly(expectedBlob);
            }
            float[] expectedVector = vectors.get(id);
            if (expectedVector == null) {
                assertThat(row.isNullAt(2)).isTrue();
            } else {
                assertThat(row.getVector(2).toFloatArray()).containsExactly(expectedVector);
            }
        }
    }

    private Table createTable(String format, String... sharedShreddingFields) throws Exception {
        return createTable(format, 2, sharedShreddingFields);
    }

    private Table createTable(String format, int maxColumns, String... sharedShreddingFields)
            throws Exception {
        return createTableWithBucket(format, maxColumns, "-1", sharedShreddingFields);
    }

    private Table createTableWithBucket(
            String format, int maxColumns, String bucket, String... sharedShreddingFields)
            throws Exception {
        return createTableWithBucket(
                format,
                maxColumns,
                bucket,
                Arrays.asList(sharedShreddingFields),
                Arrays.asList(sharedShreddingFields));
    }

    private Table createTableWithBucket(
            String format,
            int maxColumns,
            String bucket,
            List<String> mapFields,
            List<String> sharedShreddingFields)
            throws Exception {
        catalog.createTable(
                identifier(format),
                schemaWithBucket(format, maxColumns, bucket, mapFields, sharedShreddingFields),
                true);
        return catalog.getTable(identifier(format));
    }

    private Table createDataEvolutionTable(String format, String... sharedShreddingFields)
            throws Exception {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT());
        for (String field : sharedShreddingFields) {
            builder.column(field, DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT()));
        }
        builder.option("bucket", "-1")
                .option("file.format", format)
                .option(CoreOptions.WRITE_ONLY.key(), "true")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        for (String field : sharedShreddingFields) {
            builder.option("fields." + field + ".map.storage-layout", "shared-shredding")
                    .option("fields." + field + ".map.shared-shredding.max-columns", "2");
        }
        catalog.createTable(identifier(format), builder.build(), true);
        return catalog.getTable(identifier(format));
    }

    private Table createBlobAndVectorTable(String format) throws Exception {
        catalog.createTable(
                identifier(format),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT()))
                        .column("payload", DataTypes.BLOB())
                        .column("embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))
                        .option("bucket", "-1")
                        .option("file.format", format)
                        .option(CoreOptions.FILE_COMPRESSION.key(), "none")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.VECTOR_FILE_FORMAT.key(), "json")
                        .option("fields.metrics.map.storage-layout", "shared-shredding")
                        .option("fields.metrics.map.shared-shredding.max-columns", "2")
                        .build(),
                true);
        return catalog.getTable(identifier(format));
    }

    private Table createCompactingAppendOnlyTable(String format) throws Exception {
        catalog.createTable(
                identifier(format),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT()))
                        .option("bucket", "1")
                        .option("bucket-key", "id")
                        .option("file.format", format)
                        .option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "99")
                        .option("fields.metrics.map.storage-layout", "shared-shredding")
                        .option("fields.metrics.map.shared-shredding.max-columns", "64")
                        .build(),
                true);
        return catalog.getTable(identifier(format));
    }

    private Table createPrimaryKeyTable(String format, boolean thinMode, int maxColumns)
            throws Exception {
        return createPrimaryKeyTable(format, thinMode, maxColumns, true);
    }

    private Table createPrimaryKeyTable(
            String format, boolean thinMode, int maxColumns, boolean writeOnly) throws Exception {
        return createPrimaryKeyTable(format, thinMode, maxColumns, writeOnly, false);
    }

    private Table createPrimaryKeyTable(
            String format,
            boolean thinMode,
            int maxColumns,
            boolean writeOnly,
            boolean deletionVectors)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT()));
        if (deletionVectors) {
            builder.column("padding", DataTypes.STRING())
                    .option(CoreOptions.TARGET_FILE_SIZE.key(), "1 kb")
                    .option(CoreOptions.FILE_COMPRESSION.key(), "none");
        }
        builder.primaryKey("id")
                .option("bucket", "1")
                .option("bucket-key", "id")
                .option("file.format", format)
                .option(CoreOptions.WRITE_ONLY.key(), String.valueOf(writeOnly))
                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), String.valueOf(deletionVectors))
                .option(CoreOptions.DATA_FILE_THIN_MODE.key(), String.valueOf(thinMode))
                .option("fields.metrics.map.storage-layout", "shared-shredding")
                .option(
                        "fields.metrics.map.shared-shredding.max-columns",
                        String.valueOf(maxColumns));
        catalog.createTable(identifier(format), builder.build(), true);
        return catalog.getTable(identifier(format));
    }

    private Table createPrimaryKeyInputChangelogTable(
            String dataFileFormat, String changelogFileFormat) throws Exception {
        catalog.createTable(
                identifier(dataFileFormat),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT()))
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("bucket-key", "id")
                        .option("file.format", dataFileFormat)
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option(CoreOptions.CHANGELOG_FILE_FORMAT.key(), changelogFileFormat)
                        .option(CoreOptions.FILE_COMPRESSION.key(), "zstd")
                        .option(CoreOptions.CHANGELOG_FILE_COMPRESSION.key(), "zstd")
                        .option("fields.metrics.map.storage-layout", "shared-shredding")
                        .option("fields.metrics.map.shared-shredding.max-columns", "2")
                        .build(),
                true);
        return catalog.getTable(identifier(dataFileFormat));
    }

    private Table createPrimaryKeyAggregationTable(String format, boolean thinMode)
            throws Exception {
        catalog.createTable(
                identifier(format),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT()))
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("bucket-key", "id")
                        .option("file.format", format)
                        .option(CoreOptions.DATA_FILE_THIN_MODE.key(), String.valueOf(thinMode))
                        .option(CoreOptions.MERGE_ENGINE.key(), "aggregation")
                        .option("fields.metrics.aggregate-function", "merge_map")
                        .option("fields.metrics.map.storage-layout", "shared-shredding")
                        .option("fields.metrics.map.shared-shredding.max-columns", "2")
                        .build(),
                true);
        return catalog.getTable(identifier(format));
    }

    private Map<Integer, Map<String, Long>> readMapsById(ReadBuilder readBuilder) throws Exception {
        Map<Integer, Map<String, Long>> result = new LinkedHashMap<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row ->
                            result.put(
                                    row.getInt(0),
                                    row.isNullAt(1) ? null : toJavaMap(row.getMap(1))));
        }
        return result;
    }

    private List<CommitMessage> compactAndCommit(
            Table table, BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            write.compact(partition, bucket, fullCompaction);
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
            return messages;
        }
    }

    private List<CommitMessage> writeAndCommit(Table table, InternalRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            for (InternalRow row : rows) {
                write.write(row);
            }
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
            return messages;
        }
    }

    private Table createComplexValueTable(String format) throws Exception {
        catalog.createTable(
                identifier(format),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(
                                "metrics",
                                DataTypes.MAP(
                                        DataTypes.STRING().notNull(),
                                        DataTypes.ROW(
                                                DataTypes.FIELD(0, "count", DataTypes.BIGINT()),
                                                DataTypes.FIELD(
                                                        1,
                                                        "tags",
                                                        DataTypes.ARRAY(DataTypes.STRING())),
                                                DataTypes.FIELD(
                                                        2,
                                                        "attrs",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.BIGINT())))))
                        .option("bucket", "-1")
                        .option("file.format", format)
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option("fields.metrics.map.storage-layout", "shared-shredding")
                        .option("fields.metrics.map.shared-shredding.max-columns", "2")
                        .build(),
                true);
        return catalog.getTable(identifier(format));
    }

    private Table createAllSupportedValueTypesTable(String format) throws Exception {
        catalog.createTable(
                identifier(format),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(
                                "metrics",
                                DataTypes.MAP(
                                        DataTypes.STRING().notNull(),
                                        DataTypes.ROW(
                                                DataTypes.FIELD(0, "bool", DataTypes.BOOLEAN()),
                                                DataTypes.FIELD(1, "tiny", DataTypes.TINYINT()),
                                                DataTypes.FIELD(2, "small", DataTypes.SMALLINT()),
                                                DataTypes.FIELD(3, "i", DataTypes.INT()),
                                                DataTypes.FIELD(4, "big", DataTypes.BIGINT()),
                                                DataTypes.FIELD(5, "f", DataTypes.FLOAT()),
                                                DataTypes.FIELD(6, "d", DataTypes.DOUBLE()),
                                                DataTypes.FIELD(7, "s", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        8, "varchar_value", DataTypes.VARCHAR(32)),
                                                DataTypes.FIELD(9, "char_value", DataTypes.CHAR(6)),
                                                DataTypes.FIELD(10, "bin", DataTypes.BINARY(3)),
                                                DataTypes.FIELD(
                                                        11, "var_bin", DataTypes.VARBINARY(8)),
                                                DataTypes.FIELD(
                                                        12,
                                                        "compact_decimal",
                                                        DataTypes.DECIMAL(10, 2)),
                                                DataTypes.FIELD(
                                                        13,
                                                        "large_decimal",
                                                        DataTypes.DECIMAL(23, 5)),
                                                DataTypes.FIELD(14, "date", DataTypes.DATE()),
                                                DataTypes.FIELD(15, "time", DataTypes.TIME()),
                                                DataTypes.FIELD(16, "ts3", DataTypes.TIMESTAMP(3)),
                                                DataTypes.FIELD(17, "ts9", DataTypes.TIMESTAMP(9)),
                                                DataTypes.FIELD(
                                                        18,
                                                        "ts_ltz3",
                                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(
                                                                3)),
                                                DataTypes.FIELD(
                                                        19,
                                                        "ts_ltz6",
                                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(
                                                                6)),
                                                DataTypes.FIELD(
                                                        20,
                                                        "ints",
                                                        DataTypes.ARRAY(DataTypes.INT())),
                                                DataTypes.FIELD(
                                                        21,
                                                        "attrs",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.BIGINT())),
                                                DataTypes.FIELD(
                                                        22,
                                                        "nested",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        0,
                                                                        "name",
                                                                        DataTypes.STRING()),
                                                                DataTypes.FIELD(
                                                                        1,
                                                                        "score",
                                                                        DataTypes.INT()))))))
                        .option("bucket", "-1")
                        .option("file.format", format)
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option("fields.metrics.map.storage-layout", "shared-shredding")
                        .option("fields.metrics.map.shared-shredding.max-columns", "2")
                        .build(),
                true);
        return catalog.getTable(identifier(format));
    }

    private void writeWithWriteType(Table table, RowType writeType, InternalRow... rows)
            throws Exception {
        writeWithWriteType(table, writeType, null, rows);
    }

    private void writeWithWriteType(
            Table table, RowType writeType, Long firstRowId, InternalRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                BatchTableCommit commit = builder.newCommit()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            List<CommitMessage> commitMessages = write.prepareCommit();
            if (firstRowId != null) {
                setFirstRowId(commitMessages, firstRowId);
            }
            commit.commit(commitMessages);
        }
    }

    private void setFirstRowId(List<CommitMessage> commitMessages, long firstRowId) {
        for (CommitMessage message : commitMessages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            List<DataFileMeta> newFiles =
                    new ArrayList<>(commitMessage.newFilesIncrement().newFiles());
            commitMessage.newFilesIncrement().newFiles().clear();
            for (DataFileMeta newFile : newFiles) {
                commitMessage
                        .newFilesIncrement()
                        .newFiles()
                        .add(newFile.assignFirstRowId(firstRowId));
            }
        }
    }

    private Schema schema(String format, String... sharedShreddingFields) {
        return schema(format, 2, sharedShreddingFields);
    }

    private Schema schema(String format, int maxColumns, String... sharedShreddingFields) {
        return schemaWithBucket(format, maxColumns, "-1", sharedShreddingFields);
    }

    private Schema schemaWithBucket(
            String format, int maxColumns, String bucket, String... sharedShreddingFields) {
        return schemaWithBucket(
                format,
                maxColumns,
                bucket,
                Arrays.asList(sharedShreddingFields),
                Arrays.asList(sharedShreddingFields));
    }

    private Schema schemaWithBucket(
            String format,
            int maxColumns,
            String bucket,
            List<String> mapFields,
            List<String> sharedShreddingFields) {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT());
        for (String field : mapFields) {
            builder.column(field, DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT()));
        }
        builder.option("bucket", bucket)
                .option("file.format", format)
                .option(CoreOptions.WRITE_ONLY.key(), "true");
        if (!"-1".equals(bucket)) {
            builder.option("bucket-key", "id");
        }
        for (String field : sharedShreddingFields) {
            builder.option("fields." + field + ".map.storage-layout", "shared-shredding")
                    .option(
                            "fields." + field + ".map.shared-shredding.max-columns",
                            String.valueOf(maxColumns));
        }
        return builder.build();
    }

    private List<DataFileWithSplit> currentDataFiles(FileStoreTable table) throws Exception {
        List<DataFileWithSplit> files = new ArrayList<>();
        for (DataSplit split : table.newSnapshotReader().read().dataSplits()) {
            for (DataFileMeta dataFile : split.dataFiles()) {
                files.add(new DataFileWithSplit(split.partition(), split.bucket(), dataFile));
            }
        }
        return files;
    }

    private MapSharedShreddingFieldMeta readSharedShreddingFieldMeta(
            FileStoreTable table, DataFileWithSplit file, String fieldName) throws Exception {
        DataFilePathFactory pathFactory =
                table.store().pathFactory().createDataFilePathFactory(file.partition, file.bucket);
        FileFormat fileFormat =
                FileFormatDiscover.of(new CoreOptions(table.options()))
                        .discover(file.dataFile.fileFormat());
        Map<String, Map<String, String>> fieldMetadata =
                ((SupportsFieldMetadata) fileFormat)
                        .readFieldMetadata(
                                new FormatReaderContext(
                                        table.fileIO(),
                                        pathFactory.toPath(file.dataFile),
                                        file.dataFile.fileSize()));
        return MapSharedShreddingUtils.deserializeMetadata(fieldMetadata.get(fieldName));
    }

    private GenericMap mapOf(Object... entries) {
        Map<BinaryString, Long> map = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put(BinaryString.fromString((String) entries[i]), (Long) entries[i + 1]);
        }
        return new GenericMap(map);
    }

    private GenericMap complexMapOf(Object... entries) {
        Map<BinaryString, GenericRow> map = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put(BinaryString.fromString((String) entries[i]), (GenericRow) entries[i + 1]);
        }
        return new GenericMap(map);
    }

    private GenericMap wideMapOf(Object... entries) {
        Map<BinaryString, GenericRow> map = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put(BinaryString.fromString((String) entries[i]), (GenericRow) entries[i + 1]);
        }
        return new GenericMap(map);
    }

    private GenericRow complexValue(Long count, GenericArray tags, GenericMap attrs) {
        return GenericRow.of(count, tags, attrs);
    }

    private GenericRow toWideRow(WideValue value) {
        return GenericRow.of(
                value.bool,
                value.tiny,
                value.small,
                value.i,
                value.big,
                value.f,
                value.d,
                value.s == null ? null : BinaryString.fromString(value.s),
                value.varcharValue == null ? null : BinaryString.fromString(value.varcharValue),
                value.charValue == null ? null : BinaryString.fromString(value.charValue),
                value.bin,
                value.varBin,
                value.compactDecimal,
                value.largeDecimal,
                value.date,
                value.time,
                value.ts3,
                value.ts9,
                value.tsLtz3,
                value.tsLtz6,
                value.ints == null ? null : new GenericArray(value.ints.toArray(new Integer[0])),
                value.attrs == null ? null : longMapFromJava(value.attrs),
                value.nested == null
                        ? null
                        : GenericRow.of(
                                BinaryString.fromString(value.nested.name), value.nested.score));
    }

    private GenericArray stringArray(String... values) {
        BinaryString[] strings = new BinaryString[values.length];
        for (int i = 0; i < values.length; i++) {
            strings[i] = BinaryString.fromString(values[i]);
        }
        return new GenericArray(strings);
    }

    private GenericMap longMapOf(Object... entries) {
        Map<BinaryString, Long> map = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put(BinaryString.fromString((String) entries[i]), (Long) entries[i + 1]);
        }
        return new GenericMap(map);
    }

    private GenericMap longMapFromJava(Map<String, Long> values) {
        Map<BinaryString, Long> map = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            map.put(BinaryString.fromString(entry.getKey()), entry.getValue());
        }
        return new GenericMap(map);
    }

    private Map<String, Long> toJavaMap(InternalMap map) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (int i = 0; i < map.size(); i++) {
            result.put(
                    map.keyArray().getString(i).toString(),
                    map.valueArray().isNullAt(i) ? null : map.valueArray().getLong(i));
        }
        return result;
    }

    private long numberOfSpillFiles() throws Exception {
        try (Stream<java.nio.file.Path> paths = Files.walk(tempPath)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".channel"))
                    .count();
        }
    }

    private Map<String, Long> javaMapOf(Object... entries) {
        Map<String, Long> map = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put((String) entries[i], (Long) entries[i + 1]);
        }
        return map;
    }

    private Map<String, ComplexValue> toJavaComplexMap(InternalMap map) {
        Map<String, ComplexValue> result = new LinkedHashMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < map.size(); i++) {
            result.put(
                    keys.getString(i).toString(),
                    values.isNullAt(i) ? null : toJavaComplexValue(values.getRow(i, 3)));
        }
        return result;
    }

    private ComplexValue toJavaComplexValue(InternalRow row) {
        return new ComplexValue(
                row.isNullAt(0) ? null : row.getLong(0),
                row.isNullAt(1) ? null : toJavaStringList(row.getArray(1)),
                row.isNullAt(2) ? null : toJavaLongMap(row.getMap(2)));
    }

    private List<String> toJavaStringList(InternalArray array) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            result.add(array.isNullAt(i) ? null : array.getString(i).toString());
        }
        return result;
    }

    private Map<String, Long> toJavaLongMap(InternalMap map) {
        Map<String, Long> result = new LinkedHashMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < map.size(); i++) {
            result.put(keys.getString(i).toString(), values.isNullAt(i) ? null : values.getLong(i));
        }
        return result;
    }

    private Map<String, ComplexValue> javaComplexMapOf(Object... entries) {
        Map<String, ComplexValue> map = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put((String) entries[i], (ComplexValue) entries[i + 1]);
        }
        return map;
    }

    private Map<String, WideValue> toJavaWideMap(InternalMap map) {
        Map<String, WideValue> result = new LinkedHashMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < map.size(); i++) {
            result.put(
                    keys.getString(i).toString(),
                    values.isNullAt(i) ? null : toJavaWideValue(values.getRow(i, 23)));
        }
        return result;
    }

    private WideValue toJavaWideValue(InternalRow row) {
        return new WideValue(
                row.isNullAt(0) ? null : row.getBoolean(0),
                row.isNullAt(1) ? null : row.getByte(1),
                row.isNullAt(2) ? null : row.getShort(2),
                row.isNullAt(3) ? null : row.getInt(3),
                row.isNullAt(4) ? null : row.getLong(4),
                row.isNullAt(5) ? null : row.getFloat(5),
                row.isNullAt(6) ? null : row.getDouble(6),
                row.isNullAt(7) ? null : row.getString(7).toString(),
                row.isNullAt(8) ? null : row.getString(8).toString(),
                row.isNullAt(9) ? null : row.getString(9).toString(),
                row.isNullAt(10) ? null : row.getBinary(10),
                row.isNullAt(11) ? null : row.getBinary(11),
                row.isNullAt(12) ? null : row.getDecimal(12, 10, 2),
                row.isNullAt(13) ? null : row.getDecimal(13, 23, 5),
                row.isNullAt(14) ? null : row.getInt(14),
                row.isNullAt(15) ? null : row.getInt(15),
                row.isNullAt(16) ? null : row.getTimestamp(16, 3),
                row.isNullAt(17) ? null : row.getTimestamp(17, 9),
                row.isNullAt(18) ? null : row.getTimestamp(18, 3),
                row.isNullAt(19) ? null : row.getTimestamp(19, 6),
                row.isNullAt(20) ? null : toJavaIntList(row.getArray(20)),
                row.isNullAt(21) ? null : toJavaLongMap(row.getMap(21)),
                row.isNullAt(22) ? null : toJavaNestedValue(row.getRow(22, 2)));
    }

    private List<Integer> toJavaIntList(InternalArray array) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            result.add(array.isNullAt(i) ? null : array.getInt(i));
        }
        return result;
    }

    private NestedValue toJavaNestedValue(InternalRow row) {
        return new NestedValue(
                row.isNullAt(0) ? null : row.getString(0).toString(),
                row.isNullAt(1) ? null : row.getInt(1));
    }

    private Map<String, WideValue> javaWideMapOf(Object... entries) {
        Map<String, WideValue> map = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put((String) entries[i], (WideValue) entries[i + 1]);
        }
        return map;
    }

    @Override
    protected Schema schemaDefault() {
        return schema("parquet", "metrics");
    }

    private static class DataFileWithSplit {

        private final BinaryRow partition;
        private final int bucket;
        private final DataFileMeta dataFile;

        private DataFileWithSplit(BinaryRow partition, int bucket, DataFileMeta dataFile) {
            this.partition = partition;
            this.bucket = bucket;
            this.dataFile = dataFile;
        }
    }

    private static class ComplexValue {

        private final Long count;
        private final List<String> tags;
        private final Map<String, Long> attrs;

        private ComplexValue(Long count, List<String> tags, Map<String, Long> attrs) {
            this.count = count;
            this.tags = tags;
            this.attrs = attrs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ComplexValue)) {
                return false;
            }
            ComplexValue that = (ComplexValue) o;
            return java.util.Objects.equals(count, that.count)
                    && java.util.Objects.equals(tags, that.tags)
                    && java.util.Objects.equals(attrs, that.attrs);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(count, tags, attrs);
        }

        @Override
        public String toString() {
            return "ComplexValue{" + "count=" + count + ", tags=" + tags + ", attrs=" + attrs + '}';
        }
    }

    private static class NestedValue {

        private final String name;
        private final Integer score;

        private NestedValue(String name, Integer score) {
            this.name = name;
            this.score = score;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof NestedValue)) {
                return false;
            }
            NestedValue that = (NestedValue) o;
            return java.util.Objects.equals(name, that.name)
                    && java.util.Objects.equals(score, that.score);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, score);
        }

        @Override
        public String toString() {
            return "NestedValue{" + "name='" + name + '\'' + ", score=" + score + '}';
        }
    }

    private static class WideValue {

        private final Boolean bool;
        private final Byte tiny;
        private final Short small;
        private final Integer i;
        private final Long big;
        private final Float f;
        private final Double d;
        private final String s;
        private final String varcharValue;
        private final String charValue;
        private final byte[] bin;
        private final byte[] varBin;
        private final Decimal compactDecimal;
        private final Decimal largeDecimal;
        private final Integer date;
        private final Integer time;
        private final Timestamp ts3;
        private final Timestamp ts9;
        private final Timestamp tsLtz3;
        private final Timestamp tsLtz6;
        private final List<Integer> ints;
        private final Map<String, Long> attrs;
        private final NestedValue nested;

        private WideValue(
                Boolean bool,
                Byte tiny,
                Short small,
                Integer i,
                Long big,
                Float f,
                Double d,
                String s,
                String varcharValue,
                String charValue,
                byte[] bin,
                byte[] varBin,
                Decimal compactDecimal,
                Decimal largeDecimal,
                Integer date,
                Integer time,
                Timestamp ts3,
                Timestamp ts9,
                Timestamp tsLtz3,
                Timestamp tsLtz6,
                List<Integer> ints,
                Map<String, Long> attrs,
                NestedValue nested) {
            this.bool = bool;
            this.tiny = tiny;
            this.small = small;
            this.i = i;
            this.big = big;
            this.f = f;
            this.d = d;
            this.s = s;
            this.varcharValue = varcharValue;
            this.charValue = charValue;
            this.bin = bin;
            this.varBin = varBin;
            this.compactDecimal = compactDecimal;
            this.largeDecimal = largeDecimal;
            this.date = date;
            this.time = time;
            this.ts3 = ts3;
            this.ts9 = ts9;
            this.tsLtz3 = tsLtz3;
            this.tsLtz6 = tsLtz6;
            this.ints = ints;
            this.attrs = attrs;
            this.nested = nested;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof WideValue)) {
                return false;
            }
            WideValue wideValue = (WideValue) o;
            return java.util.Objects.equals(bool, wideValue.bool)
                    && java.util.Objects.equals(tiny, wideValue.tiny)
                    && java.util.Objects.equals(small, wideValue.small)
                    && java.util.Objects.equals(i, wideValue.i)
                    && java.util.Objects.equals(big, wideValue.big)
                    && java.util.Objects.equals(f, wideValue.f)
                    && java.util.Objects.equals(d, wideValue.d)
                    && java.util.Objects.equals(s, wideValue.s)
                    && java.util.Objects.equals(varcharValue, wideValue.varcharValue)
                    && java.util.Objects.equals(charValue, wideValue.charValue)
                    && Arrays.equals(bin, wideValue.bin)
                    && Arrays.equals(varBin, wideValue.varBin)
                    && java.util.Objects.equals(compactDecimal, wideValue.compactDecimal)
                    && java.util.Objects.equals(largeDecimal, wideValue.largeDecimal)
                    && java.util.Objects.equals(date, wideValue.date)
                    && java.util.Objects.equals(time, wideValue.time)
                    && java.util.Objects.equals(ts3, wideValue.ts3)
                    && java.util.Objects.equals(ts9, wideValue.ts9)
                    && java.util.Objects.equals(tsLtz3, wideValue.tsLtz3)
                    && java.util.Objects.equals(tsLtz6, wideValue.tsLtz6)
                    && java.util.Objects.equals(ints, wideValue.ints)
                    && java.util.Objects.equals(attrs, wideValue.attrs)
                    && java.util.Objects.equals(nested, wideValue.nested);
        }

        @Override
        public int hashCode() {
            int result =
                    java.util.Objects.hash(
                            bool,
                            tiny,
                            small,
                            i,
                            big,
                            f,
                            d,
                            s,
                            varcharValue,
                            charValue,
                            compactDecimal,
                            largeDecimal,
                            date,
                            time,
                            ts3,
                            ts9,
                            tsLtz3,
                            tsLtz6,
                            ints,
                            attrs,
                            nested);
            result = 31 * result + Arrays.hashCode(bin);
            result = 31 * result + Arrays.hashCode(varBin);
            return result;
        }

        @Override
        public String toString() {
            return "WideValue{"
                    + "bool="
                    + bool
                    + ", tiny="
                    + tiny
                    + ", small="
                    + small
                    + ", i="
                    + i
                    + ", big="
                    + big
                    + ", f="
                    + f
                    + ", d="
                    + d
                    + ", s='"
                    + s
                    + '\''
                    + ", varcharValue='"
                    + varcharValue
                    + '\''
                    + ", charValue='"
                    + charValue
                    + '\''
                    + ", bin="
                    + Arrays.toString(bin)
                    + ", varBin="
                    + Arrays.toString(varBin)
                    + ", compactDecimal="
                    + compactDecimal
                    + ", largeDecimal="
                    + largeDecimal
                    + ", date="
                    + date
                    + ", time="
                    + time
                    + ", ts3="
                    + ts3
                    + ", ts9="
                    + ts9
                    + ", tsLtz3="
                    + tsLtz3
                    + ", tsLtz6="
                    + tsLtz6
                    + ", ints="
                    + ints
                    + ", attrs="
                    + attrs
                    + ", nested="
                    + nested
                    + '}';
        }
    }
}
