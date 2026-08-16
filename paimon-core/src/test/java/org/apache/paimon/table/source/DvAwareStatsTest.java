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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DvAwareStats}. */
public class DvAwareStatsTest {

    @Test
    public void testNoDvAndEmptyDeleteRowCountIsTight() {
        DataFileMeta file = DataFileTestUtils.newFile();
        assertThat(DvAwareStats.isTightBounds(file, null)).isTrue();
    }

    @Test
    public void testNoDvAndPositiveDeleteRowCountIsWide() {
        DataFileMeta file = DataFileTestUtils.newFile("f1", 0, 0, 9, 10L, 5L);
        assertThat(DvAwareStats.isTightBounds(file, null)).isFalse();
    }

    @Test
    public void testEmptyDvIsTight() {
        DataFileMeta file = DataFileTestUtils.newFile();
        DeletionFile dv = new DeletionFile("dv-1", 0L, 0L, 0L);
        assertThat(DvAwareStats.isTightBounds(file, dv)).isTrue();
    }

    @Test
    public void testPopulatedDvIsWide() {
        DataFileMeta file = DataFileTestUtils.newFile();
        DeletionFile dv = new DeletionFile("dv-1", 0L, 16L, 5L);
        assertThat(DvAwareStats.isTightBounds(file, dv)).isFalse();
    }

    @Test
    public void testUnknownCardinalityDvIsConservativelyWide() {
        DataFileMeta file = DataFileTestUtils.newFile();
        DeletionFile dv = new DeletionFile("dv-1", 0L, 16L, null);
        assertThat(DvAwareStats.isTightBounds(file, dv)).isFalse();
    }

    @Test
    public void testNoDvAndZeroDeleteRowCountIsTight() {
        DataFileMeta file = DataFileTestUtils.newFile("f1", 0, 0, 9, 10L, 0L);
        assertThat(DvAwareStats.isTightBounds(file, null)).isTrue();
    }

    @Test
    public void testBothDeleteRowCountAndPopulatedDvIsWide() {
        DataFileMeta file = DataFileTestUtils.newFile("f1", 0, 0, 9, 10L, 5L);
        DeletionFile dv = new DeletionFile("dv-1", 0L, 16L, 3L);
        assertThat(DvAwareStats.isTightBounds(file, dv)).isFalse();
    }
}
