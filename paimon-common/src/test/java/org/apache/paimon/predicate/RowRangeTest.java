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

package org.apache.paimon.predicate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowRange}, focused on {@link RowRange#localOf}. */
public class RowRangeTest {

    /** No global range set -> read the whole file (null local range). */
    @Test
    public void testNullGlobalRangeReturnsNull() {
        assertThat(RowRange.localOf(null, 0, 100)).isNull();
        assertThat(RowRange.localOf(null, 50, 10)).isNull();
    }

    /** The file fully contains the requested range. */
    @Test
    public void testRangeFullyInsideFile() {
        // global [10, 19] over a 100-row file starting at global 0 -> local [10, 19].
        RowRange local = RowRange.localOf(RowRange.of(10, 19), 0, 100);
        assertThat(local).isEqualTo(RowRange.of(10, 19));
    }

    /** The range starts before this file: local start is clamped to 0. */
    @Test
    public void testRangeStartsBeforeFile() {
        // global [5, 25] over a 20-row file starting at global 10 -> local [0, 15].
        RowRange local = RowRange.localOf(RowRange.of(5, 25), 10, 20);
        assertThat(local).isEqualTo(RowRange.of(0, 15));
    }

    /** The range ends after this file: local end is clamped to the file's last row. */
    @Test
    public void testRangeEndsAfterFile() {
        // global [10, 200] over a 20-row file starting at global 10 -> local [0, 19].
        RowRange local = RowRange.localOf(RowRange.of(10, 200), 10, 20);
        assertThat(local).isEqualTo(RowRange.of(0, 19));
    }

    /** The file lies entirely before the range -> EMPTY (skip the file). */
    @Test
    public void testFileEntirelyBeforeRange() {
        // global [50, 60] over a 20-row file starting at global 0 -> EMPTY.
        RowRange local = RowRange.localOf(RowRange.of(50, 60), 0, 20);
        assertThat(local).isSameAs(RowRange.EMPTY);
    }

    /** The file lies entirely after the range -> EMPTY (skip the file). */
    @Test
    public void testFileEntirelyAfterRange() {
        // global [0, 5] over a 20-row file starting at global 10 -> start clamps to 0 but
        // end = 5 - 10 = -5 < 0 -> EMPTY.
        RowRange local = RowRange.localOf(RowRange.of(0, 5), 10, 20);
        assertThat(local).isSameAs(RowRange.EMPTY);
    }

    /** The range touches only the first row of the file. */
    @Test
    public void testRangeCoversOnlyFirstRow() {
        // global [10, 10] over a 20-row file starting at global 10 -> local [0, 0].
        RowRange local = RowRange.localOf(RowRange.of(10, 10), 10, 20);
        assertThat(local).isEqualTo(RowRange.of(0, 0));
        assertThat(local.count()).isEqualTo(1L);
    }

    /** The range touches only the last row of the file. */
    @Test
    public void testRangeCoversOnlyLastRow() {
        // global [29, 29] over a 20-row file starting at global 10 -> local [19, 19].
        RowRange local = RowRange.localOf(RowRange.of(29, 29), 10, 20);
        assertThat(local).isEqualTo(RowRange.of(19, 19));
    }

    /** An unbounded end reads to the file's last row. */
    @Test
    public void testUnboundedEndClampsToLastRow() {
        // global [15, MAX] over a 20-row file starting at global 10 -> local [5, 19].
        RowRange local = RowRange.localOf(RowRange.of(15, Long.MAX_VALUE), 10, 20);
        assertThat(local).isEqualTo(RowRange.of(5, 19));
        assertThat(local.count()).isEqualTo(15L);
    }

    /** FULL range (from 0 to MAX) over a file starting at a non-zero offset. */
    @Test
    public void testFullRangeOverOffsetFile() {
        RowRange local = RowRange.localOf(RowRange.FULL, 30, 20);
        assertThat(local).isEqualTo(RowRange.of(0, 19));
    }

    /** A range that starts exactly at the file boundary. */
    @Test
    public void testRangeStartsAtFileBoundary() {
        // global [10, 15] over a 20-row file starting at global 10 -> local [0, 5].
        RowRange local = RowRange.localOf(RowRange.of(10, 15), 10, 20);
        assertThat(local).isEqualTo(RowRange.of(0, 5));
    }

    /** A range that ends exactly at the file boundary. */
    @Test
    public void testRangeEndsAtFileBoundary() {
        // global [25, 29] over a 20-row file starting at global 10 -> local [15, 19].
        RowRange local = RowRange.localOf(RowRange.of(25, 29), 10, 20);
        assertThat(local).isEqualTo(RowRange.of(15, 19));
    }

    /** start exactly at fileRowCount (one past the last row) -> EMPTY. */
    @Test
    public void testStartAtFileRowCount() {
        RowRange local = RowRange.localOf(RowRange.of(30, 40), 10, 20);
        assertThat(local).isSameAs(RowRange.EMPTY);
    }

    /** Multiple files in a split: each local range is computed against its own offset. */
    @Test
    public void testMultipleFilesConcat() {
        // Split: file0 [0,99] (100 rows), file1 [100,199] (100 rows). Global range [50, 149].
        RowRange global = RowRange.of(50, 149);
        RowRange local0 = RowRange.localOf(global, 0, 100);
        RowRange local1 = RowRange.localOf(global, 100, 100);
        assertThat(local0).isEqualTo(RowRange.of(50, 99)); // last 50 rows of file0
        assertThat(local1).isEqualTo(RowRange.of(0, 49)); // first 50 rows of file1
        assertThat(local0.count() + local1.count()).isEqualTo(100L);
    }

    /** A file in the middle of the split that is fully outside the range. */
    @Test
    public void testMiddleFileFullyOutside() {
        // Split: file0 [0,49], file1 [50,99], file2 [100,149]. Global range [60, 140].
        RowRange global = RowRange.of(60, 140);
        assertThat(RowRange.localOf(global, 0, 50)).isSameAs(RowRange.EMPTY); // file0 before range
        assertThat(RowRange.localOf(global, 50, 50))
                .isEqualTo(RowRange.of(10, 49)); // file1 partial
        assertThat(RowRange.localOf(global, 100, 50))
                .isEqualTo(RowRange.of(0, 40)); // file2 partial
    }
}
