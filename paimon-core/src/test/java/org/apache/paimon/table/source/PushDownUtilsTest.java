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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PushDownUtils}. */
public class PushDownUtilsTest {

    @Test
    public void testNonDataSplitReturnsFalse() {
        Split nonDataSplit =
                new Split() {
                    @Override
                    public long rowCount() {
                        return 0;
                    }

                    @Override
                    public OptionalLong mergedRowCount() {
                        return OptionalLong.empty();
                    }
                };
        assertThat(PushDownUtils.tightBoundsAvailable(nonDataSplit)).isFalse();
    }

    @Test
    public void testEmptySplitReturnsTrue() {
        DataSplit split = newSplit(Collections.emptyList(), null);
        assertThat(PushDownUtils.tightBoundsAvailable(split)).isTrue();
    }

    @Test
    public void testAllFilesTightReturnsTrue() {
        List<DataFileMeta> files =
                Arrays.asList(
                        DataFileTestUtils.newFile(),
                        DataFileTestUtils.newFile(),
                        DataFileTestUtils.newFile());
        DataSplit split = newSplit(files, null);
        assertThat(PushDownUtils.tightBoundsAvailable(split)).isTrue();
    }

    @Test
    public void testAnyFileWithPopulatedDvReturnsFalse() {
        List<DataFileMeta> files =
                Arrays.asList(
                        DataFileTestUtils.newFile(),
                        DataFileTestUtils.newFile(),
                        DataFileTestUtils.newFile());
        List<DeletionFile> dvs =
                Arrays.asList(
                        new DeletionFile("dv-0", 0L, 0L, 0L),
                        new DeletionFile("dv-1", 0L, 16L, 5L),
                        new DeletionFile("dv-2", 0L, 0L, 0L));
        DataSplit split = newSplit(files, dvs);
        assertThat(PushDownUtils.tightBoundsAvailable(split)).isFalse();
    }

    @Test
    public void testFileWithDeleteRowCountReturnsFalse() {
        List<DataFileMeta> files =
                Arrays.asList(
                        DataFileTestUtils.newFile("f0", 0, 0, 9, 10L, 5L),
                        DataFileTestUtils.newFile(),
                        DataFileTestUtils.newFile());
        List<DeletionFile> dvs =
                Arrays.asList(
                        new DeletionFile("dv-0", 0L, 0L, 0L),
                        new DeletionFile("dv-1", 0L, 0L, 0L),
                        new DeletionFile("dv-2", 0L, 0L, 0L));
        DataSplit split = newSplit(files, dvs);
        assertThat(PushDownUtils.tightBoundsAvailable(split)).isFalse();
    }

    private static DataSplit newSplit(List<DataFileMeta> files, List<DeletionFile> deletionFiles) {
        DataSplit.Builder builder =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("dummy")
                        .withDataFiles(files);
        if (deletionFiles != null) {
            builder.withDataDeletionFiles(deletionFiles);
        }
        return builder.build();
    }
}
