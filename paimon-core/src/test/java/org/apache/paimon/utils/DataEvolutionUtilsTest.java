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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
}
