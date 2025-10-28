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

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryArrayWriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.DataEvolutionArray;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataEvolutionFileStoreScan}. */
public class DataEvolutionFileStoreScanTest {

    private Map<Long, TableSchema> schemas;
    private Function<Long, TableSchema> scanTableSchema;

    @BeforeEach
    public void setUp() {
        schemas = new HashMap<>();
        scanTableSchema = schemas::get;
    }

    @Test
    public void testEvolutionStatsSingleFile() {
        Schema schema = createSchema("f0", "f1");
        TableSchema tableSchema = TableSchema.create(0L, schema);
        schemas.put(0L, tableSchema);

        ManifestEntry entry =
                createManifestEntry(
                        0L,
                        createSimpleStats(
                                GenericRow.of(1, BinaryString.fromString("a")),
                                GenericRow.of(5, BinaryString.fromString("z")),
                                createBinaryArray(new int[] {0, 1}),
                                new int[] {0, 1}));

        SimpleStatsEvolution.Result result =
                DataEvolutionFileStoreScan.evolutionStats(
                        tableSchema, scanTableSchema, Collections.singletonList(entry));

        assertThat(result).isNotNull();
        assertThat(result.minValues()).isInstanceOf(DataEvolutionRow.class);
        assertThat(result.maxValues()).isInstanceOf(DataEvolutionRow.class);
        assertThat(result.nullCounts()).isInstanceOf(DataEvolutionArray.class);

        DataEvolutionRow minRow = (DataEvolutionRow) result.minValues();
        DataEvolutionRow maxRow = (DataEvolutionRow) result.maxValues();
        DataEvolutionArray nullCounts = (DataEvolutionArray) result.nullCounts();

        assertThat(minRow.rowNumber()).isEqualTo(1);
        assertThat(maxRow.rowNumber()).isEqualTo(1);
        assertThat(nullCounts.size()).isEqualTo(2);

        assertThat(minRow.getInt(0)).isEqualTo(1);
        assertThat(maxRow.getInt(0)).isEqualTo(5);
        assertThat(minRow.getString(1).toString()).isEqualTo("a");
        assertThat(maxRow.getString(1).toString()).isEqualTo("z");

        assertThat(nullCounts.getInt(0)).isEqualTo(0);
        assertThat(nullCounts.getInt(1)).isEqualTo(1);

        assertThat(minRow.getFieldCount()).isEqualTo(2);
        assertThat(maxRow.getFieldCount()).isEqualTo(2);
    }

    @Test
    public void testEvolutionStatsMultipleFiles() {
        Schema schema = createSchema("f0", "f1", "f2");
        TableSchema tableSchema = TableSchema.create(0L, schema);
        schemas.put(0L, tableSchema);
        schemas.put(1L, tableSchema.project(Arrays.asList("f0", "f2")));

        ManifestEntry entry1 =
                createManifestEntry(
                        0L,
                        createSimpleStats(
                                GenericRow.of(1, BinaryString.fromString("a"), 10),
                                GenericRow.of(3, BinaryString.fromString("c"), 30),
                                createBinaryArray(new int[] {0, 1, 0}),
                                new int[] {0, 1, 2}));

        ManifestEntry entry2 =
                createManifestEntry(
                        1L,
                        createSimpleStats(
                                GenericRow.of(2, 20),
                                GenericRow.of(4, 40),
                                createBinaryArray(new int[] {1, 2}),
                                new int[] {0, 2}));

        List<ManifestEntry> entries = Arrays.asList(entry2, entry1);

        SimpleStatsEvolution.Result result =
                DataEvolutionFileStoreScan.evolutionStats(tableSchema, scanTableSchema, entries);

        assertThat(result).isNotNull();
        DataEvolutionRow minRow = (DataEvolutionRow) result.minValues();
        DataEvolutionRow maxRow = (DataEvolutionRow) result.maxValues();
        DataEvolutionArray nullCounts = (DataEvolutionArray) result.nullCounts();

        assertThat(minRow.getInt(0)).isEqualTo(2);
        assertThat(maxRow.getInt(0)).isEqualTo(4);
        assertThat(minRow.getInt(2)).isEqualTo(20);
        assertThat(maxRow.getInt(2)).isEqualTo(40);
        assertThat(minRow.getString(1).toString()).isEqualTo("a");
        assertThat(maxRow.getString(1).toString()).isEqualTo("c");
        assertThat(nullCounts.getInt(0)).isEqualTo(1);
        assertThat(nullCounts.getInt(1)).isEqualTo(1);
        assertThat(nullCounts.getInt(2)).isEqualTo(2);
    }

    @Test
    public void testEvolutionStatsWithSchemaEvolution() {
        Schema baseSchema = createSchema("f0", "f1");
        TableSchema baseTableSchema = TableSchema.create(0L, baseSchema);
        schemas.put(0L, baseTableSchema);

        Schema evolvedSchema = createSchema("f0", "f1", "f2");
        TableSchema evolvedTableSchema = TableSchema.create(1L, evolvedSchema);
        schemas.put(1L, evolvedTableSchema);

        ManifestEntry entry1 =
                createManifestEntry(
                        0L,
                        createSimpleStats(
                                GenericRow.of(1, BinaryString.fromString("a")),
                                GenericRow.of(3, BinaryString.fromString("c")),
                                createBinaryArray(new int[] {0, 1}),
                                new int[] {0, 1}));

        ManifestEntry entry2 =
                createManifestEntry(
                        1L,
                        createSimpleStats(
                                GenericRow.of(2, BinaryString.fromString("b"), 20),
                                GenericRow.of(4, BinaryString.fromString("d"), 40),
                                createBinaryArray(new int[] {1, 0, 1}),
                                new int[] {0, 1, 2}));

        List<ManifestEntry> entries = Arrays.asList(entry1, entry2);

        SimpleStatsEvolution.Result result =
                DataEvolutionFileStoreScan.evolutionStats(
                        evolvedTableSchema, scanTableSchema, entries);

        assertThat(result).isNotNull();
        DataEvolutionRow minRow = (DataEvolutionRow) result.minValues();
        DataEvolutionRow maxRow = (DataEvolutionRow) result.maxValues();
        DataEvolutionArray nullCounts = (DataEvolutionArray) result.nullCounts();

        assertThat(minRow.getInt(0)).isEqualTo(1);
        assertThat(maxRow.getInt(0)).isEqualTo(3);

        assertThat(minRow.getString(1).toString()).isEqualTo("a");
        assertThat(maxRow.getString(1).toString()).isEqualTo("c");

        assertThat(minRow.getInt(2)).isEqualTo(20);
        assertThat(maxRow.getInt(2)).isEqualTo(40);

        assertThat(nullCounts.getInt(0)).isEqualTo(0);
        assertThat(nullCounts.getInt(1)).isEqualTo(1);
        assertThat(nullCounts.getInt(2)).isEqualTo(1);
    }

    @Test
    public void testEvolutionStatsWithWriteColsNotEqualToValueStatsCols() {
        Schema schema = createSchema("f0", "f1", "f2");
        TableSchema tableSchema = TableSchema.create(0L, schema);
        schemas.put(0L, tableSchema);
        schemas.put(1L, tableSchema);

        ManifestEntry entry1 =
                createManifestEntryWithDifferentCols(
                        0L,
                        new String[] {"f0", "f1", "f2"},
                        new String[] {"f0", "f1"},
                        createSimpleStats(
                                GenericRow.of(1, BinaryString.fromString("a")),
                                GenericRow.of(3, BinaryString.fromString("c")),
                                createBinaryArray(new int[] {0, 1}),
                                new int[] {0, 1}));

        ManifestEntry entry2 =
                createManifestEntryWithDifferentCols(
                        1L,
                        new String[] {"f0", "f2"},
                        new String[] {"f0", "f2"},
                        createSimpleStats(
                                GenericRow.of(2, 20),
                                GenericRow.of(4, 40),
                                createBinaryArray(new int[] {1, 0}),
                                new int[] {0, 2}));

        List<ManifestEntry> entries = Arrays.asList(entry1, entry2);

        SimpleStatsEvolution.Result result =
                DataEvolutionFileStoreScan.evolutionStats(tableSchema, scanTableSchema, entries);

        assertThat(result).isNotNull();
        DataEvolutionRow minRow = (DataEvolutionRow) result.minValues();
        DataEvolutionRow maxRow = (DataEvolutionRow) result.maxValues();
        DataEvolutionArray nullCounts = (DataEvolutionArray) result.nullCounts();

        assertThat(minRow.getInt(0)).isEqualTo(1);
        assertThat(maxRow.getInt(0)).isEqualTo(3);

        assertThat(minRow.getString(1).toString()).isEqualTo("a");
        assertThat(maxRow.getString(1).toString()).isEqualTo("c");

        assertThat(minRow.isNullAt(2)).isTrue();
        assertThat(maxRow.isNullAt(2)).isTrue();

        assertThat(nullCounts.getInt(0)).isEqualTo(0);
        assertThat(nullCounts.getInt(1)).isEqualTo(1);
        assertThat(nullCounts.isNullAt(2)).isTrue();
    }

    private Schema createSchema(String... fieldNames) {
        Schema.Builder builder = Schema.newBuilder();
        for (int i = 0; i < fieldNames.length; i++) {
            if (i == 0) {
                builder.column(fieldNames[i], DataTypes.INT());
            } else if (i == 1) {
                builder.column(fieldNames[i], DataTypes.STRING());
            } else {
                builder.column(fieldNames[i], DataTypes.INT());
            }
        }
        return builder.build();
    }

    private ManifestEntry createManifestEntry(Long schemaId, SimpleStats stats) {
        DataFileMeta fileMeta =
                DataFileMeta.create(
                        "test-file.parquet",
                        100L,
                        100L,
                        createBinaryRow(1),
                        createBinaryRow(100),
                        stats,
                        stats,
                        0L,
                        0L,
                        schemaId,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        null,
                        null);

        return ManifestEntry.create(FileKind.ADD, createBinaryRow(0), 0, 0, fileMeta);
    }

    private ManifestEntry createManifestEntryWithDifferentCols(
            Long schemaId, String[] writeCols, String[] valueStatsCols, SimpleStats stats) {
        DataFileMeta fileMeta =
                DataFileMeta.create(
                        "test-file.parquet",
                        100L,
                        100L,
                        createBinaryRow(1),
                        createBinaryRow(100),
                        stats,
                        stats,
                        0L,
                        0L,
                        schemaId,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        FileSource.APPEND,
                        Arrays.stream(valueStatsCols).collect(Collectors.toList()),
                        null,
                        null,
                        Arrays.stream(writeCols).collect(Collectors.toList()));

        return ManifestEntry.create(FileKind.ADD, createBinaryRow(0), 0, 0, fileMeta);
    }

    private BinaryRow createBinaryRow(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }

    private BinaryArray createBinaryArray(int[] values) {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, values.length, 4);
        for (int i = 0; i < values.length; i++) {
            writer.writeInt(i, values[i]);
        }
        writer.complete();
        return array;
    }

    private SimpleStats createSimpleStats(
            InternalRow minValues, InternalRow maxValues, BinaryArray nullCounts, int[] fields) {
        return new SimpleStats(
                convertToBinaryRow(minValues, fields),
                convertToBinaryRow(maxValues, fields),
                nullCounts);
    }

    private BinaryRow convertToBinaryRow(InternalRow row, int[] fields) {
        BinaryRow binaryRow = new BinaryRow(fields.length);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        for (int i = 0; i < fields.length; i++) {
            int fieldId = fields[i];
            if (i >= row.getFieldCount() || row.isNullAt(i)) {
                writer.setNullAt(i);
            } else {
                if (fieldId == 0) {
                    writer.writeInt(i, row.getInt(i));
                } else if (fieldId == 1) {
                    writer.writeString(i, row.getString(i));
                } else {
                    writer.writeInt(i, row.getInt(i));
                }
            }
        }
        writer.complete();
        return binaryRow;
    }
}
