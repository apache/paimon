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
import org.apache.paimon.operation.DataEvolutionFileStoreScan.EvolutionStats;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.DataEvolutionArray;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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

        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        tableSchema,
                        scanTableSchema,
                        Collections.singletonList(entry),
                        new EvolutionStatsCache());

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

        assertThat(nullCounts.getLong(0)).isEqualTo(0L);
        assertThat(nullCounts.getLong(1)).isEqualTo(1L);

        assertThat(minRow.getFieldCount()).isEqualTo(2);
        assertThat(maxRow.getFieldCount()).isEqualTo(2);
    }

    @Test
    public void testEvolutionStatsReusesProjectedSchema() {
        Schema schema = createSchema("f0", "f1");
        TableSchema tableSchema = TableSchema.create(0L, schema);
        schemas.put(0L, tableSchema);

        AtomicInteger schemaLoads = new AtomicInteger();
        Function<Long, TableSchema> countingScanTableSchema =
                schemaId -> {
                    schemaLoads.incrementAndGet();
                    return schemas.get(schemaId);
                };
        EvolutionStatsCache cache = new EvolutionStatsCache();
        ManifestEntry entry =
                createManifestEntry(
                        0L,
                        createSimpleStats(
                                GenericRow.of(1, BinaryString.fromString("a")),
                                GenericRow.of(5, BinaryString.fromString("z")),
                                createBinaryArray(new int[] {0, 1}),
                                new int[] {0, 1}));

        DataEvolutionFileStoreScan.evolutionStats(
                tableSchema, countingScanTableSchema, Collections.singletonList(entry), cache);
        DataEvolutionFileStoreScan.evolutionStats(
                tableSchema, countingScanTableSchema, Collections.singletonList(entry), cache);

        assertThat(schemaLoads).hasValue(1);
        assertThat(cache.size()).isEqualTo(1);
    }

    @Test
    public void testEvolutionStatsCacheSeparatesStatsProjections() {
        Schema schema = createSchema("f0", "f1");
        TableSchema tableSchema = TableSchema.create(0L, schema);
        schemas.put(0L, tableSchema);
        EvolutionStatsCache cache = new EvolutionStatsCache();

        ManifestEntry f0StatsEntry =
                createManifestEntryWithDifferentCols(
                        0L,
                        new String[] {"f0", "f1"},
                        new String[] {"f0"},
                        createSimpleStats(
                                GenericRow.of(1),
                                GenericRow.of(5),
                                createBinaryArray(new int[] {0}),
                                new int[] {0}));
        ManifestEntry f1StatsEntry =
                createManifestEntryWithDifferentCols(
                        0L,
                        new String[] {"f0", "f1"},
                        new String[] {"f1"},
                        createSimpleStats(
                                GenericRow.of(BinaryString.fromString("a")),
                                GenericRow.of(BinaryString.fromString("z")),
                                createBinaryArray(new int[] {0}),
                                new int[] {1}));

        EvolutionStats f0Stats =
                DataEvolutionFileStoreScan.evolutionStats(
                        tableSchema,
                        scanTableSchema,
                        Collections.singletonList(f0StatsEntry),
                        cache);
        EvolutionStats f1Stats =
                DataEvolutionFileStoreScan.evolutionStats(
                        tableSchema,
                        scanTableSchema,
                        Collections.singletonList(f1StatsEntry),
                        cache);

        DataEvolutionRow f0Min = (DataEvolutionRow) f0Stats.minValues();
        DataEvolutionRow f1Min = (DataEvolutionRow) f1Stats.minValues();
        assertThat(f0Min.getInt(0)).isEqualTo(1);
        assertThat(f0Min.isNullAt(1)).isTrue();
        assertThat(f1Min.isNullAt(0)).isTrue();
        assertThat(f1Min.getString(1).toString()).isEqualTo("a");
        assertThat(cache.size()).isEqualTo(2);
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
                                new int[] {0, 2}),
                        "newer.parquet",
                        1L,
                        0L,
                        100L);

        List<ManifestEntry> entries = Arrays.asList(entry2, entry1);

        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        tableSchema, scanTableSchema, entries, new EvolutionStatsCache());

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
        assertThat(nullCounts.getLong(0)).isEqualTo(1L);
        assertThat(nullCounts.getLong(1)).isEqualTo(1L);
        assertThat(nullCounts.getLong(2)).isEqualTo(2L);
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
                                new int[] {0, 1}),
                        "base-newer.parquet",
                        1L,
                        0L,
                        100L);

        ManifestEntry entry2 =
                createManifestEntry(
                        1L,
                        createSimpleStats(
                                GenericRow.of(2, BinaryString.fromString("b"), 20),
                                GenericRow.of(4, BinaryString.fromString("d"), 40),
                                createBinaryArray(new int[] {1, 0, 1}),
                                new int[] {0, 1, 2}));

        List<ManifestEntry> entries = Arrays.asList(entry1, entry2);

        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        evolvedTableSchema, scanTableSchema, entries, new EvolutionStatsCache());

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

        assertThat(nullCounts.getLong(0)).isEqualTo(0L);
        assertThat(nullCounts.getLong(1)).isEqualTo(1L);
        assertThat(nullCounts.getLong(2)).isEqualTo(1L);
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
                                new int[] {0, 1}),
                        1L);

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

        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        tableSchema, scanTableSchema, entries, new EvolutionStatsCache());

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

        assertThat(nullCounts.getLong(0)).isEqualTo(0L);
        assertThat(nullCounts.getLong(1)).isEqualTo(1L);
        assertThat(nullCounts.isNullAt(2)).isTrue();
    }

    @Test
    public void testEvolutionStatsSkipsStatsAfterColumnTypeChange() {
        Schema baseSchema = createSchema("f0", "f1");
        TableSchema baseTableSchema = TableSchema.create(0L, baseSchema);
        schemas.put(0L, baseTableSchema);

        Schema evolvedSchema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.STRING())
                        .build();
        TableSchema evolvedTableSchema = TableSchema.create(1L, evolvedSchema);
        schemas.put(1L, evolvedTableSchema);

        ManifestEntry oldTypeEntry =
                createManifestEntry(
                        0L,
                        createSimpleStats(
                                GenericRow.of(10, BinaryString.fromString("a")),
                                GenericRow.of(99, BinaryString.fromString("z")),
                                createBinaryArray(new int[] {0, 0}),
                                new int[] {0, 1}));

        BinaryRow newTypeMin = new BinaryRow(2);
        BinaryRowWriter newTypeMinWriter = new BinaryRowWriter(newTypeMin);
        newTypeMinWriter.writeString(0, BinaryString.fromString("apple"));
        newTypeMinWriter.writeString(1, BinaryString.fromString("banana"));
        newTypeMinWriter.complete();
        BinaryRow newTypeMax = new BinaryRow(2);
        BinaryRowWriter newTypeMaxWriter = new BinaryRowWriter(newTypeMax);
        newTypeMaxWriter.writeString(0, BinaryString.fromString("yam"));
        newTypeMaxWriter.writeString(1, BinaryString.fromString("zebra"));
        newTypeMaxWriter.complete();
        SimpleStats newTypeStats =
                new SimpleStats(newTypeMin, newTypeMax, createBinaryArray(new int[] {0, 0}));
        ManifestEntry newTypeEntry =
                createManifestEntry(1L, newTypeStats, "new-type.parquet", 1L, 0L, 100L);

        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        evolvedTableSchema,
                        scanTableSchema,
                        Arrays.asList(oldTypeEntry, newTypeEntry),
                        new EvolutionStatsCache());

        DataEvolutionRow minRow = (DataEvolutionRow) result.minValues();
        DataEvolutionRow maxRow = (DataEvolutionRow) result.maxValues();

        assertThat(minRow.getString(0).toString()).isEqualTo("apple");
        assertThat(maxRow.getString(0).toString()).isEqualTo("yam");
    }

    @Test
    public void testTypeChangedColumnMustNotPrunePreAlterFiles() {
        Schema baseSchema = createSchema("f0", "f1");
        schemas.put(0L, TableSchema.create(0L, baseSchema));

        Schema evolvedSchema =
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())
                        .column("f1", DataTypes.STRING())
                        .build();
        TableSchema evolvedTableSchema = TableSchema.create(1L, evolvedSchema);
        schemas.put(1L, evolvedTableSchema);

        ManifestEntry preAlterFile =
                createManifestEntry(
                        0L,
                        createSimpleStats(
                                GenericRow.of(10, BinaryString.fromString("a")),
                                GenericRow.of(99, BinaryString.fromString("z")),
                                createBinaryArray(new int[] {0, 0}),
                                new int[] {0, 1}));

        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        evolvedTableSchema,
                        scanTableSchema,
                        Collections.singletonList(preAlterFile),
                        new EvolutionStatsCache());

        Predicate onChangedColumn =
                new PredicateBuilder(evolvedTableSchema.logicalRowType()).equal(0, 50L);

        boolean keepFile =
                onChangedColumn.test(
                        result.rowCount(),
                        result.minValues(),
                        result.maxValues(),
                        result.nullCounts());

        assertThat(keepFile)
                .as("pre-ALTER file must not be pruned by a predicate on a type-changed column")
                .isTrue();
    }

    @Test
    public void testEvolutionStatsKeepDedicatedVectorFieldAsUnknown() {
        Schema schema = createSchema("f0", "f1", "f2");
        TableSchema tableSchema = TableSchema.create(0L, schema);
        schemas.put(0L, tableSchema);

        ManifestEntry dataEntry =
                createManifestEntryWithDifferentColsAndFileName(
                        "data-file.parquet",
                        0L,
                        new String[] {"f0", "f1"},
                        new String[] {"f0", "f1"},
                        createSimpleStats(
                                GenericRow.of(1, BinaryString.fromString("a")),
                                GenericRow.of(3, BinaryString.fromString("c")),
                                createBinaryArray(new int[] {0, 0}),
                                new int[] {0, 1}));

        ManifestEntry vectorEntry =
                createManifestEntryWithDifferentColsAndFileName(
                        "data-file.vector.avro",
                        0L,
                        new String[] {"f2"},
                        new String[] {"f2"},
                        createSimpleStats(
                                GenericRow.of(10),
                                GenericRow.of(30),
                                createBinaryArray(new int[] {0}),
                                new int[] {2}));

        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        tableSchema,
                        scanTableSchema,
                        Arrays.asList(dataEntry, vectorEntry),
                        new EvolutionStatsCache());

        DataEvolutionArray nullCounts = (DataEvolutionArray) result.nullCounts();
        assertThat(nullCounts.isNullAt(2)).isTrue();

        Predicate predicate = new PredicateBuilder(tableSchema.logicalRowType()).isNotNull(2);
        assertThat(
                        predicate.test(
                                result.rowCount(),
                                result.minValues(),
                                result.maxValues(),
                                result.nullCounts()))
                .isTrue();
    }

    @Test
    public void testNewestIncompatibleProviderIsUnknown() {
        Schema oldSchema = createSchema("f0");
        schemas.put(0L, TableSchema.create(0L, oldSchema));
        TableSchema currentSchema =
                TableSchema.create(
                        1L, Schema.newBuilder().column("f0", DataTypes.BIGINT()).build());
        schemas.put(1L, currentSchema);

        ManifestEntry compatible =
                createManifestEntry(
                        1L,
                        createSimpleStats(
                                GenericRow.of(10),
                                GenericRow.of(20),
                                createBinaryArray(new int[] {0}),
                                new int[] {0}),
                        "compatible.parquet",
                        1L,
                        0L,
                        10L);
        ManifestEntry newerIncompatible =
                createManifestEntry(
                        0L,
                        createSimpleStats(
                                GenericRow.of(100),
                                GenericRow.of(200),
                                createBinaryArray(new int[] {0}),
                                new int[] {0}),
                        "incompatible.parquet",
                        2L,
                        0L,
                        10L);

        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        currentSchema,
                        scanTableSchema,
                        Arrays.asList(compatible, newerIncompatible),
                        new EvolutionStatsCache());

        assertThat(result.minValues().isNullAt(0)).isTrue();
        assertThat(result.nullCounts().isNullAt(0)).isTrue();
    }

    @Test
    public void testPartialLatestProviderIsUnknown() {
        Schema schema = createSchema("f0");
        TableSchema tableSchema = TableSchema.create(0L, schema);
        schemas.put(0L, tableSchema);
        SimpleStats baseStats =
                createSimpleStats(
                        GenericRow.of(0),
                        GenericRow.of(9),
                        createBinaryArray(new int[] {0}),
                        new int[] {0});
        SimpleStats partialStats =
                createSimpleStats(
                        GenericRow.of(100),
                        GenericRow.of(104),
                        createBinaryArray(new int[] {0}),
                        new int[] {0});

        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        tableSchema,
                        scanTableSchema,
                        Arrays.asList(
                                createManifestEntry(0L, baseStats, "base.parquet", 0L, 0L, 10L),
                                createManifestEntry(
                                        0L, partialStats, "partial.parquet", 1L, 5L, 5L)),
                        new EvolutionStatsCache());

        assertThat(result.minValues().isNullAt(0)).isTrue();
        assertThat(result.nullCounts().isNullAt(0)).isTrue();
    }

    @Test
    public void testTiedLatestProvidersAreUnknown() {
        Schema schema = createSchema("f0");
        TableSchema tableSchema = TableSchema.create(0L, schema);
        schemas.put(0L, tableSchema);
        SimpleStats stats =
                createSimpleStats(
                        GenericRow.of(0),
                        GenericRow.of(9),
                        createBinaryArray(new int[] {0}),
                        new int[] {0});

        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        tableSchema,
                        scanTableSchema,
                        Arrays.asList(
                                createManifestEntry(0L, stats, "first.parquet", 1L, 0L, 10L),
                                createManifestEntry(0L, stats, "second.parquet", 1L, 0L, 10L)),
                        new EvolutionStatsCache());

        assertThat(result.minValues().isNullAt(0)).isTrue();
        assertThat(result.nullCounts().isNullAt(0)).isTrue();
    }

    @Test
    public void testInvalidProviderStatsAreUnknown() {
        Schema schema = createSchema("f0");
        TableSchema tableSchema = TableSchema.create(0L, schema);
        schemas.put(0L, tableSchema);

        assertInvalidProviderStatsAreUnknown(tableSchema, 10, 10, -1);
        assertInvalidProviderStatsAreUnknown(tableSchema, 10, 10, 11);
        assertInvalidProviderStatsAreUnknown(tableSchema, null, 10, 0);
        assertInvalidProviderStatsAreUnknown(tableSchema, 10, null, 0);
        assertInvalidProviderStatsAreUnknown(tableSchema, 10, 1, 0);
        assertInvalidProviderStatsAreUnknown(tableSchema, 10, 10, 10);
    }

    private void assertInvalidProviderStatsAreUnknown(
            TableSchema tableSchema, Object min, Object max, int nullCount) {
        SimpleStats stats =
                createSimpleStats(
                        GenericRow.of(min),
                        GenericRow.of(max),
                        createBinaryArray(new int[] {nullCount}),
                        new int[] {0});
        EvolutionStats result =
                DataEvolutionFileStoreScan.evolutionStats(
                        tableSchema,
                        scanTableSchema,
                        Collections.singletonList(
                                createManifestEntry(0L, stats, "invalid.parquet", 0L, 0L, 10L)),
                        new EvolutionStatsCache());

        assertThat(result.minValues().isNullAt(0)).isTrue();
        assertThat(result.maxValues().isNullAt(0)).isTrue();
        assertThat(result.nullCounts().isNullAt(0)).isTrue();
        Predicate predicate = new PredicateBuilder(tableSchema.logicalRowType()).equal(0, 5);
        assertThat(
                        predicate.test(
                                result.rowCount(),
                                result.minValues(),
                                result.maxValues(),
                                result.nullCounts()))
                .isTrue();
    }

    @Test
    public void testIntersectsRowRanges() {
        List<Range> rowRanges =
                Arrays.asList(
                        new Range(20, 30), new Range(0, 10), new Range(5, 15), new Range(35, 40));
        RowRangeIndex index = RowRangeIndex.create(rowRanges);

        assertThat(index.intersects(14, 14)).isTrue();
        assertThat(index.intersects(16, 19)).isFalse();
        assertThat(index.intersects(31, 34)).isFalse();
        assertThat(index.intersects(29, 31)).isTrue();
        assertThat(index.intersects(100, 200)).isFalse();
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
        return createManifestEntry(schemaId, stats, "test-file.parquet", 0L, 0L, 100L);
    }

    private ManifestEntry createManifestEntry(
            Long schemaId,
            SimpleStats stats,
            String fileName,
            long sequence,
            long firstRowId,
            long rowCount) {
        DataFileMeta fileMeta =
                DataFileMeta.create(
                        fileName,
                        100L,
                        rowCount,
                        createBinaryRow(1),
                        createBinaryRow(100),
                        stats,
                        stats,
                        sequence,
                        sequence,
                        schemaId,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        firstRowId,
                        null);

        return ManifestEntry.create(FileKind.ADD, createBinaryRow(0), 0, 0, fileMeta);
    }

    private ManifestEntry createManifestEntryWithDifferentCols(
            Long schemaId, String[] writeCols, String[] valueStatsCols, SimpleStats stats) {
        return createManifestEntryWithDifferentCols(schemaId, writeCols, valueStatsCols, stats, 0L);
    }

    private ManifestEntry createManifestEntryWithDifferentCols(
            Long schemaId,
            String[] writeCols,
            String[] valueStatsCols,
            SimpleStats stats,
            long sequence) {
        return createManifestEntryWithDifferentColsAndFileName(
                "test-file.parquet", schemaId, writeCols, valueStatsCols, stats, sequence);
    }

    private ManifestEntry createManifestEntryWithDifferentColsAndFileName(
            String fileName,
            Long schemaId,
            String[] writeCols,
            String[] valueStatsCols,
            SimpleStats stats) {
        return createManifestEntryWithDifferentColsAndFileName(
                fileName, schemaId, writeCols, valueStatsCols, stats, 0L);
    }

    private ManifestEntry createManifestEntryWithDifferentColsAndFileName(
            String fileName,
            Long schemaId,
            String[] writeCols,
            String[] valueStatsCols,
            SimpleStats stats,
            long sequence) {
        DataFileMeta fileMeta =
                DataFileMeta.create(
                        fileName,
                        100L,
                        100L,
                        createBinaryRow(1),
                        createBinaryRow(100),
                        stats,
                        stats,
                        sequence,
                        sequence,
                        schemaId,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        FileSource.APPEND,
                        Arrays.stream(valueStatsCols).collect(Collectors.toList()),
                        null,
                        0L,
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
        BinaryArrayWriter writer = new BinaryArrayWriter(array, values.length, 8);
        for (int i = 0; i < values.length; i++) {
            writer.writeLong(i, values[i]);
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
