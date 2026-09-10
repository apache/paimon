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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DataEvolutionUtils}. */
public class DataEvolutionUtilsTest {

    @Test
    public void testFileFieldIdsIgnoresSystemFields() {
        TableSchema schema =
                new TableSchema(
                        1L,
                        Arrays.asList(
                                new DataField(1, "indexed", new IntType()),
                                new DataField(2, "other", new IntType())),
                        2,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");

        assertThat(
                        DataEvolutionUtils.fileFieldIds(
                                schema.fields(),
                                dataFile(
                                        "mixed.parquet",
                                        1,
                                        Arrays.asList(
                                                SpecialFields.ROW_ID.name(),
                                                "indexed",
                                                SpecialFields.SEQUENCE_NUMBER.name())),
                                false))
                .containsExactly(1);
        assertThat(
                        DataEvolutionUtils.fileFieldIds(
                                schema.fields(),
                                dataFile(
                                        "system-only.parquet",
                                        1,
                                        Arrays.asList(
                                                SpecialFields.ROW_ID.name(),
                                                SpecialFields.SEQUENCE_NUMBER.name())),
                                false))
                .isEmpty();
    }

    @Test
    public void testFileFieldIdsHandlesFullEmptyAndUnrelatedWrites() {
        TableSchema schema =
                new TableSchema(
                        1L,
                        Arrays.asList(
                                new DataField(1, "indexed", new IntType()),
                                new DataField(2, "other", new IntType())),
                        2,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");

        assertThat(
                        DataEvolutionUtils.fileFieldIds(
                                schema.fields(), dataFile("full.parquet", 1, null), false))
                .containsExactlyInAnyOrder(1, 2);
        assertThat(
                        DataEvolutionUtils.fileFieldIds(
                                schema.fields(),
                                dataFile("empty.parquet", 1, Collections.emptyList()),
                                false))
                .isEmpty();
        assertThat(
                        DataEvolutionUtils.fileFieldIds(
                                schema.fields(),
                                dataFile(
                                        "unrelated.parquet", 1, Collections.singletonList("other")),
                                false))
                .containsExactly(2);
        assertThat(
                        DataEvolutionUtils.fileFieldIds(
                                schema.fields(),
                                dataFile(
                                        "unknown.parquet", 1, Collections.singletonList("unknown")),
                                false))
                .isEmpty();
    }

    @Test
    public void testNestedWriteColumnResolutionRequiresEnabledOption() {
        DataField nested =
                new DataField(
                        1,
                        "nest",
                        DataTypes.ROW(
                                new DataField(2, "a", DataTypes.INT()),
                                new DataField(3, "b", DataTypes.INT())));
        TableSchema disabled = tableSchema(1L, Collections.emptyMap(), nested);
        DataFileMeta nestedFile =
                dataFile("nested.parquet", 1L, Collections.singletonList("nest.a"));

        assertThat(DataEvolutionUtils.fileFieldIds(disabled.fields(), nestedFile, false)).isEmpty();
        assertThat(collectWrittenColumnIds(ignored -> disabled, nestedFile)).isEmpty();

        Map<String, String> enabledOptions = new HashMap<>();
        enabledOptions.put(CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
        TableSchema enabled = tableSchema(1L, enabledOptions, nested);
        assertThat(DataEvolutionUtils.fileFieldIds(enabled.fields(), nestedFile, true))
                .containsExactly(1);
        assertThat(collectWrittenColumnIds(ignored -> enabled, nestedFile))
                .hasValue(Collections.singletonList(1));
    }

    @Test
    public void testDottedTopLevelWriteColumnWinsOverNestedPath() {
        TableSchema schema =
                tableSchema(
                        1L,
                        Collections.emptyMap(),
                        new DataField(1, "nest.a", DataTypes.INT()),
                        new DataField(
                                2, "nest", DataTypes.ROW(new DataField(3, "a", DataTypes.INT()))));
        DataFileMeta file = dataFile("dotted.parquet", 1L, Collections.singletonList("nest.a"));

        assertThat(DataEvolutionUtils.fileFieldIds(schema.fields(), file, false))
                .containsExactly(1);
        assertThat(DataEvolutionUtils.fileFields(schema.fields(), file, false))
                .extracting(DataField::id)
                .containsExactly(1);
        assertThat(DataEvolutionUtils.fileFields(schema.fields(), file, true))
                .extracting(DataField::id)
                .containsExactly(1);
        assertThat(collectWrittenColumnIds(ignored -> schema, file))
                .hasValue(Collections.singletonList(1));
    }

    @Test
    public void testCollectWrittenColumnIdsAcrossSchemas() {
        Map<Long, TableSchema> schemas = new HashMap<>();
        schemas.put(
                0L,
                tableSchema(
                        0L,
                        new DataField(1, "a", DataTypes.INT()),
                        new DataField(2, "old_name", DataTypes.STRING())));
        schemas.put(
                1L,
                tableSchema(
                        1L,
                        new DataField(2, "new_name", DataTypes.STRING()),
                        new DataField(3, "c", DataTypes.BIGINT())));

        DataFileMeta oldSchemaFile = dataFile(0L, Arrays.asList("a", "old_name"));
        DataFileMeta newSchemaFile = dataFile(1L, Arrays.asList("new_name", "c"));

        assertThat(collectWrittenColumnIds(schemas::get, oldSchemaFile, newSchemaFile))
                .hasValue(Arrays.asList(1, 2, 3));
    }

    @Test
    public void testCollectWrittenColumnIdsUsesNestedOptionOfEachSchema() {
        Map<Long, TableSchema> schemas = new HashMap<>();
        schemas.put(
                0L,
                tableSchema(
                        0L, Collections.emptyMap(), new DataField(1, "nest.a", DataTypes.INT())));
        Map<String, String> nestedOptions = new HashMap<>();
        nestedOptions.put(CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
        schemas.put(
                1L,
                tableSchema(
                        1L,
                        nestedOptions,
                        new DataField(
                                2, "nest", DataTypes.ROW(new DataField(3, "b", DataTypes.INT())))));

        DataFileMeta dottedTopLevelFile = dataFile(0L, Collections.singletonList("nest.a"));
        DataFileMeta nestedFile = dataFile(1L, Collections.singletonList("nest.b"));

        assertThat(collectWrittenColumnIds(schemas::get, dottedTopLevelFile, nestedFile))
                .hasValue(Arrays.asList(1, 2));
    }

    @Test
    public void testCollectWrittenColumnIdsFallsBackWhenResolutionFails() {
        DataFileMeta unknownSchemaFile = dataFile(99L, Collections.singletonList("a"));
        assertThat(collectWrittenColumnIds(ignored -> null, unknownSchemaFile))
                .as("unknown schema")
                .isEmpty();

        DataFileMeta unresolvedSchemaFile = dataFile(1L, Collections.singletonList("missing"));
        assertThat(
                        collectWrittenColumnIds(
                                ignored -> {
                                    throw new IllegalArgumentException("schema cannot be resolved");
                                },
                                unresolvedSchemaFile))
                .as("schema loader failure")
                .isEmpty();

        TableSchema schema = tableSchema(1L, new DataField(1, "a", DataTypes.INT()));
        DataFileMeta unknownColumnFile = dataFile(1L, Collections.singletonList("missing"));
        assertThat(collectWrittenColumnIds(ignored -> schema, unknownColumnFile))
                .as("unknown non-system write column")
                .isEmpty();
    }

    @Test
    public void testCollectWrittenColumnIdsIgnoresSystemFields() {
        TableSchema schema = tableSchema(1L, new DataField(1, "a", DataTypes.INT()));
        DataFileMeta file =
                dataFile(
                        1L,
                        Arrays.asList(
                                SpecialFields.ROW_ID.name(),
                                "a",
                                SpecialFields.SEQUENCE_NUMBER.name()));

        assertThat(collectWrittenColumnIds(ignored -> schema, file))
                .hasValue(Collections.singletonList(1));

        assertThat(
                        collectWrittenColumnIds(
                                ignored -> schema,
                                dataFile(
                                        1L,
                                        Arrays.asList(
                                                SpecialFields.ROW_ID.name(),
                                                SpecialFields.SEQUENCE_NUMBER.name()))))
                .hasValue(Collections.emptyList());
    }

    @Test
    public void testCollectWrittenColumnIdsCachesSchemaAcrossProjections() {
        TableSchema schema =
                tableSchema(
                        1L,
                        new DataField(1, "a", DataTypes.INT()),
                        new DataField(2, "b", DataTypes.STRING()));
        DataFileMeta first = dataFile(1L, Collections.singletonList("a"));
        DataFileMeta second = dataFile(1L, Collections.singletonList("b"));
        DataFileMeta repeated = dataFile(1L, Collections.singletonList("a"));
        AtomicInteger schemaFieldLoads = new AtomicInteger();
        AtomicInteger nestedOptionLoads = new AtomicInteger();

        Optional<List<Integer>> result =
                DataEvolutionUtils.collectWrittenColumnIds(
                        Collections.singletonList(dataSplit(first, second, repeated)),
                        ignored -> {
                            schemaFieldLoads.incrementAndGet();
                            return schema.fields();
                        },
                        ignored -> {
                            nestedOptionLoads.incrementAndGet();
                            return false;
                        });

        assertThat(result.get()).containsExactly(1, 2);
        assertThat(schemaFieldLoads).hasValue(1);
        assertThat(nestedOptionLoads).hasValue(1);
    }

    @Test
    public void testCollectWrittenColumnIdsExpandsLegacyFileSchema() {
        TableSchema schema =
                tableSchema(
                        1L,
                        new DataField(1, "a", DataTypes.INT()),
                        new DataField(2, "b", DataTypes.STRING()));
        DataFileMeta legacyFile = dataFile(1L, null);

        assertThat(collectWrittenColumnIds(ignored -> schema, legacyFile))
                .hasValue(Arrays.asList(1, 2));
    }

    @Test
    public void testFileFieldsFollowWriteColsOrderAndIgnoreSystemFields() {
        TableSchema schema =
                new TableSchema(
                        1L,
                        Arrays.asList(
                                new DataField(1, "indexed", new IntType()),
                                new DataField(2, "other", new IntType())),
                        2,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");

        assertThat(
                        DataEvolutionUtils.fileFields(
                                schema.fields(),
                                dataFile(
                                        "reordered.parquet",
                                        1,
                                        Arrays.asList(
                                                "other", SpecialFields.ROW_ID.name(), "indexed")),
                                false))
                .extracting(DataField::id)
                .containsExactly(2, 1);
    }

    @Test
    public void testFileFieldsProjectsNestedPathsOnlyWhenEnabled() {
        DataField nested =
                new DataField(
                        1,
                        "nest",
                        DataTypes.ROW(
                                new DataField(2, "a", DataTypes.INT()),
                                new DataField(3, "b", DataTypes.INT())));
        DataFileMeta file = dataFile("nested.parquet", 1, Arrays.asList("nest.b", "nest.a"));

        TableSchema disabled = tableSchema(1L, Collections.emptyMap(), nested);
        assertThat(DataEvolutionUtils.fileFields(disabled.fields(), file, false)).isEmpty();

        Map<String, String> enabledOptions = new HashMap<>();
        enabledOptions.put(CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
        TableSchema enabled = tableSchema(1L, enabledOptions, nested);
        List<DataField> fields = DataEvolutionUtils.fileFields(enabled.fields(), file, true);
        assertThat(fields).extracting(DataField::name).containsExactly("nest");
        assertThat(((org.apache.paimon.types.RowType) fields.get(0).type()).getFieldNames())
                .containsExactly("b", "a");
    }

    @Test
    public void testFieldMaxSequenceNumberFallsBackForMissingOrMalformedArray() {
        DataFileMeta legacy = dataFile("legacy.parquet", 10, null);
        DataFileMeta malformed =
                dataFile("malformed.parquet", 10, null)
                        .withColumnMaxSequenceNumbers(new long[] {5L});
        DataFileMeta valid =
                dataFile("valid.parquet", 10, null)
                        .withColumnMaxSequenceNumbers(new long[] {5L, 8L});

        assertThat(
                        DataEvolutionUtils.fieldMaxSequenceNumber(
                                legacy, legacy.columnMaxSequenceNumbers(), 0, 2))
                .isEqualTo(10L);
        assertThat(
                        DataEvolutionUtils.fieldMaxSequenceNumber(
                                malformed, malformed.columnMaxSequenceNumbers(), 0, 2))
                .isEqualTo(10L);
        long[] validSequences = valid.columnMaxSequenceNumbers();
        assertThat(DataEvolutionUtils.fieldMaxSequenceNumber(valid, validSequences, 0, 2))
                .isEqualTo(5L);
        assertThat(DataEvolutionUtils.fieldMaxSequenceNumber(valid, validSequences, 1, 2))
                .isEqualTo(8L);
    }

    @Test
    public void testRetrieveAnchorFileSkipsSpecialFiles() {
        DataFileMeta blobFile = dataFile("blob-file.blob", 1);
        DataFileMeta vectorFile = dataFile("data.vector.lance", 2);
        DataFileMeta oldestNormalFile = dataFile("oldest-normal.parquet", 3);
        DataFileMeta newestNormalFile = dataFile("newest-normal.parquet", 4);

        assertThat(
                        DataEvolutionUtils.retrieveAnchorFile(
                                Arrays.asList(
                                        blobFile, newestNormalFile, vectorFile, oldestNormalFile),
                                Function.identity()))
                .isSameAs(oldestNormalFile);
    }

    @Test
    public void testRetrieveAnchorFileFailsWithoutNormalFile() {
        assertThatThrownBy(
                        () ->
                                DataEvolutionUtils.retrieveAnchorFile(
                                        Arrays.asList(
                                                dataFile("blob-file.blob", 1),
                                                dataFile("data.vector.lance", 2)),
                                        Function.identity()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("normal anchor file");
    }

    @Test
    public void testRetrieveAnchorFileTieBreaksWithFileName() {
        DataFileMeta largerFileName = dataFile("normal-2.parquet", 1);
        DataFileMeta smallerFileName = dataFile("normal-1.parquet", 1);

        assertThat(
                        DataEvolutionUtils.retrieveAnchorFile(
                                Arrays.asList(largerFileName, smallerFileName),
                                Function.identity()))
                .isSameAs(smallerFileName);
    }

    private static DataFileMeta dataFile(String fileName, long maxSequenceNumber) {
        return dataFile(fileName, maxSequenceNumber, Collections.emptyList());
    }

    private static DataFileMeta dataFile(
            String fileName, long maxSequenceNumber, List<String> writeCols) {
        return DataFileMeta.forAppend(
                fileName,
                1L,
                1L,
                SimpleStats.EMPTY_STATS,
                maxSequenceNumber,
                maxSequenceNumber,
                1L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                0L,
                writeCols);
    }

    private static DataFileMeta dataFile(long schemaId, java.util.List<String> writeCols) {
        return DataFileMeta.forAppend(
                "schema-" + schemaId + ".parquet",
                1L,
                1L,
                SimpleStats.EMPTY_STATS,
                1L,
                1L,
                schemaId,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                0L,
                writeCols);
    }

    private static DataSplit dataSplit(DataFileMeta... files) {
        return DataSplit.builder()
                .withSnapshot(1L)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withDataFiles(Arrays.asList(files))
                .build();
    }

    private static Optional<List<Integer>> collectWrittenColumnIds(
            Function<Long, TableSchema> schemaLoader, DataFileMeta... files) {
        Map<Long, TableSchema> schemaCache = new HashMap<>();
        Function<Long, TableSchema> cachedSchemaLoader =
                schemaId -> schemaCache.computeIfAbsent(schemaId, schemaLoader);
        return DataEvolutionUtils.collectWrittenColumnIds(
                Collections.singletonList(dataSplit(files)),
                schemaId -> {
                    TableSchema schema = cachedSchemaLoader.apply(schemaId);
                    return schema == null ? null : schema.fields();
                },
                schemaId ->
                        new CoreOptions(cachedSchemaLoader.apply(schemaId).options())
                                .dataEvolutionNestedFieldEnabled());
    }

    private static TableSchema tableSchema(long id, DataField... fields) {
        return tableSchema(id, Collections.emptyMap(), fields);
    }

    private static TableSchema tableSchema(
            long id, Map<String, String> options, DataField... fields) {
        return TableSchema.create(
                id,
                new Schema(
                        Arrays.asList(fields),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options,
                        null));
    }
}
