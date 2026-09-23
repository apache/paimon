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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class RowIdRangeConflictCheckerTest {

    @Test
    void testDetectsOverlappingRowRanges() {
        RowIdRangeConflictChecker checker =
                RowIdRangeConflictChecker.fromDataFiles(
                        Arrays.asList(file("first", 0L, 5L), file("second", 10L, 5L)));

        assertThat(checker.conflictsWith(file("same", 0L, 5L))).isTrue();
        assertThat(checker.conflictsWith(file("overlap", 4L, 7L))).isTrue();
        assertThat(checker.conflictsWith(file("contained", 11L, 2L))).isTrue();
    }

    @Test
    void testAllowsDisjointAndAdjacentRowRanges() {
        RowIdRangeConflictChecker checker =
                RowIdRangeConflictChecker.fromDataFiles(
                        Collections.singletonList(file("current", 5L, 5L)));

        assertThat(checker.conflictsWith(file("before", 0L, 5L))).isFalse();
        assertThat(checker.conflictsWith(file("after", 10L, 5L))).isFalse();
    }

    @Test
    void testIgnoresFilesWithoutRowIds() {
        RowIdRangeConflictChecker checker =
                RowIdRangeConflictChecker.fromDataFiles(
                        Collections.singletonList(file("current", null, 5L)));

        assertThat(checker.isEmpty()).isTrue();
        assertThat(checker.conflictsWith(file("historical", 0L, 5L))).isFalse();
    }

    private DataFileMeta file(String fileName, @Nullable Long firstRowId, long rowCount) {
        return DataFileMeta.forAppend(
                fileName,
                0L,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0L,
                0L,
                0L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                firstRowId,
                null);
    }
}
