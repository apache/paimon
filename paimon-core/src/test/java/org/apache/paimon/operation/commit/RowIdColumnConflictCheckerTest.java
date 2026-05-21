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

package org.apache.paimon.operation.commit;

import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.SchemaEvolutionTableTestBase.TestingSchemaManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowIdColumnConflictCheckerTest {

    @Test
    void testAllowsDisjointWriteColumns() {
        RowIdColumnConflictChecker checker =
                checker(file("current", 0L, 10L, 0L, Arrays.asList("b")));

        assertThat(checker.conflictsWith(file("historical", 0L, 10L, 0L, Arrays.asList("c"))))
                .isFalse();
    }

    @Test
    void testDetectsSameWriteColumns() {
        RowIdColumnConflictChecker checker =
                checker(file("current", 0L, 10L, 0L, Arrays.asList("b")));

        assertThat(checker.conflictsWith(file("historical", 0L, 10L, 0L, Arrays.asList("b"))))
                .isTrue();
    }

    @Test
    void testUsesFieldIdAcrossRename() {
        RowIdColumnConflictChecker checker =
                checker(file("current", 0L, 10L, 1L, Arrays.asList("b_renamed")));

        assertThat(checker.conflictsWith(file("historical", 0L, 10L, 0L, Arrays.asList("b"))))
                .isTrue();
    }

    @Test
    void testTreatsNullWriteColumnsAsFullSchemaWrite() {
        RowIdColumnConflictChecker checker = checker(file("current", 0L, 10L, 0L, null));

        assertThat(checker.conflictsWith(file("historical", 0L, 10L, 0L, Arrays.asList("b"))))
                .isTrue();
    }

    @Test
    void testMergesOverlappedDeltaRangesAndWriteColumns() {
        RowIdColumnConflictChecker checker =
                checker(
                        file("current-b", 0L, 11L, 0L, Arrays.asList("b")),
                        file("current-c", 5L, 11L, 0L, Arrays.asList("c")));

        assertThat(checker.conflictsWith(file("historical-b", 12L, 1L, 0L, Arrays.asList("b"))))
                .isTrue();
        assertThat(checker.conflictsWith(file("historical-c", 12L, 1L, 0L, Arrays.asList("c"))))
                .isTrue();
    }

    @Test
    void testScansAllOverlappedRangesAfterBinarySearch() {
        RowIdColumnConflictChecker checker =
                checker(
                        file("current-b", 0L, 5L, 0L, Arrays.asList("b")),
                        file("current-c", 10L, 5L, 0L, Arrays.asList("c")));

        assertThat(checker.conflictsWith(file("historical", 3L, 10L, 0L, Arrays.asList("c"))))
                .isTrue();
    }

    @Test
    void testFailsOnUnknownNonSystemWriteColumn() {
        RowIdColumnConflictChecker checker =
                checker(file("current", 0L, 10L, 0L, Arrays.asList("b")));

        assertThatThrownBy(
                        () ->
                                checker.conflictsWith(
                                        file("historical", 0L, 10L, 0L, Arrays.asList("missing"))))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot find write column 'missing'");
    }

    private RowIdColumnConflictChecker checker(DataFileMeta... files) {
        return RowIdColumnConflictChecker.fromDataFiles(
                createSchemaManager(), Arrays.asList(files));
    }

    private DataFileMeta file(
            String fileName,
            @Nullable Long firstRowId,
            long rowCount,
            long schemaId,
            @Nullable List<String> writeCols) {
        return DataFileMeta.forAppend(
                fileName,
                0L,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0L,
                0L,
                schemaId,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                firstRowId,
                writeCols);
    }

    private SchemaManager createSchemaManager() {
        Map<Long, org.apache.paimon.schema.TableSchema> schemas = new HashMap<>();
        schemas.put(
                0L,
                org.apache.paimon.schema.TableSchema.create(
                        0L,
                        new Schema(
                                Arrays.asList(
                                        new DataField(0, "id", DataTypes.INT()),
                                        new DataField(1, "b", DataTypes.INT()),
                                        new DataField(2, "c", DataTypes.INT())),
                                Collections.emptyList(),
                                Collections.singletonList("id"),
                                Collections.emptyMap(),
                                "")));
        schemas.put(
                1L,
                org.apache.paimon.schema.TableSchema.create(
                        1L,
                        new Schema(
                                Arrays.asList(
                                        new DataField(0, "id", DataTypes.INT()),
                                        new DataField(1, "b_renamed", DataTypes.INT()),
                                        new DataField(2, "c", DataTypes.INT())),
                                Collections.emptyList(),
                                Collections.singletonList("id"),
                                Collections.emptyMap(),
                                "")));
        return new TestingSchemaManager(
                new Path("/tmp/row-id-column-conflict-checker-test"), schemas);
    }
}
