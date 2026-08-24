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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
                                ignored -> schema,
                                dataFile(
                                        "mixed.parquet",
                                        1,
                                        Arrays.asList(
                                                SpecialFields.ROW_ID.name(),
                                                "indexed",
                                                SpecialFields.SEQUENCE_NUMBER.name()))))
                .containsExactly(1);
        assertThat(
                        DataEvolutionUtils.fileFieldIds(
                                ignored -> schema,
                                dataFile(
                                        "system-only.parquet",
                                        1,
                                        Arrays.asList(
                                                SpecialFields.ROW_ID.name(),
                                                SpecialFields.SEQUENCE_NUMBER.name()))))
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
                                ignored -> schema, dataFile("full.parquet", 1, null)))
                .containsExactlyInAnyOrder(1, 2);
        assertThat(
                        DataEvolutionUtils.fileFieldIds(
                                ignored -> schema,
                                dataFile("empty.parquet", 1, Collections.emptyList())))
                .isEmpty();
        assertThat(
                        DataEvolutionUtils.fileFieldIds(
                                ignored -> schema,
                                dataFile(
                                        "unrelated.parquet",
                                        1,
                                        Collections.singletonList("other"))))
                .containsExactly(2);
        assertThat(
                        DataEvolutionUtils.fileFieldIds(
                                ignored -> schema,
                                dataFile(
                                        "unknown.parquet",
                                        1,
                                        Collections.singletonList("unknown"))))
                .isEmpty();
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
                spy(
                        tableSchema(
                                1L,
                                new DataField(1, "a", DataTypes.INT()),
                                new DataField(2, "b", DataTypes.STRING())));
        DataFileMeta first = dataFile(1L, Collections.singletonList("a"));
        DataFileMeta second = dataFile(1L, Collections.singletonList("b"));
        DataFileMeta repeated = dataFile(1L, Collections.singletonList("a"));
        AtomicInteger schemaLoads = new AtomicInteger();

        Optional<List<Integer>> result =
                collectWrittenColumnIds(
                        ignored -> {
                            schemaLoads.incrementAndGet();
                            return schema;
                        },
                        first,
                        second,
                        repeated);

        assertThat(result.get()).containsExactly(1, 2);
        assertThat(schemaLoads).hasValue(1);
        verify(schema).fields();
        verify(repeated).writeCols();
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
                                ignored -> schema,
                                dataFile(
                                        "reordered.parquet",
                                        1,
                                        Arrays.asList(
                                                "other", SpecialFields.ROW_ID.name(), "indexed"))))
                .extracting(DataField::id)
                .containsExactly(2, 1);
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
        DataFileMeta file = mock(DataFileMeta.class);
        when(file.schemaId()).thenReturn(schemaId);
        when(file.writeCols()).thenReturn(writeCols);
        return file;
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
        return DataEvolutionUtils.collectWrittenColumnIds(
                Collections.singletonList(dataSplit(files)), schemaLoader);
    }

    private static TableSchema tableSchema(long id, DataField... fields) {
        return TableSchema.create(
                id,
                new Schema(
                        Arrays.asList(fields),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null));
    }
}
