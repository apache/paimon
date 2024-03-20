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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.BucketMode;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.io.DataFileTestUtils.fromMinMax;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AppendOnlySplitGenerator} and {@link MergeTreeSplitGenerator}. */
public class SplitGeneratorTest {

    public static DataFileMeta newFileFromSequence(
            String name, int rowCount, long minSequence, long maxSequence) {
        return new DataFileMeta(
                name,
                rowCount,
                1,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                null,
                minSequence,
                maxSequence,
                0,
                0,
                0L);
    }

    @Test
    public void testAppend() {
        List<DataFileMeta> files =
                Arrays.asList(
                        newFileFromSequence("1", 11, 0, 20),
                        newFileFromSequence("2", 13, 21, 30),
                        newFileFromSequence("3", 46, 31, 40),
                        newFileFromSequence("4", 23, 41, 50),
                        newFileFromSequence("5", 4, 51, 60),
                        newFileFromSequence("6", 101, 61, 100));
        assertThat(
                        toNames(
                                new AppendOnlySplitGenerator(40, 2, BucketMode.FIXED)
                                        .splitForBatch(files)))
                .containsExactlyInAnyOrder(
                        Arrays.asList("1", "2"),
                        Collections.singletonList("3"),
                        Arrays.asList("4", "5"),
                        Collections.singletonList("6"));

        assertThat(
                        toNames(
                                new AppendOnlySplitGenerator(70, 2, BucketMode.FIXED)
                                        .splitForBatch(files)))
                .containsExactlyInAnyOrder(
                        Arrays.asList("1", "2", "3"),
                        Arrays.asList("4", "5"),
                        Collections.singletonList("6"));

        assertThat(
                        toNames(
                                new AppendOnlySplitGenerator(40, 20, BucketMode.FIXED)
                                        .splitForBatch(files)))
                .containsExactlyInAnyOrder(
                        Arrays.asList("1", "2"),
                        Collections.singletonList("3"),
                        Collections.singletonList("4"),
                        Collections.singletonList("5"),
                        Collections.singletonList("6"));
    }

    @Test
    public void testMergeTree() {
        List<DataFileMeta> files =
                Arrays.asList(
                        fromMinMax("1", 0, 10),
                        fromMinMax("2", 0, 12),
                        fromMinMax("3", 15, 60),
                        fromMinMax("4", 18, 40),
                        fromMinMax("5", 82, 85),
                        fromMinMax("6", 100, 200));
        Comparator<InternalRow> comparator = Comparator.comparingInt(o -> o.getInt(0));
        assertThat(
                        toNames(
                                new MergeTreeSplitGenerator(comparator, 100, 2, false, DEDUPLICATE)
                                        .splitForBatch(files)))
                .containsExactlyInAnyOrder(
                        Arrays.asList("1", "2", "4", "3", "5"), Collections.singletonList("6"));

        assertThat(
                        toNames(
                                new MergeTreeSplitGenerator(comparator, 100, 30, false, DEDUPLICATE)
                                        .splitForBatch(files)))
                .containsExactlyInAnyOrder(
                        Arrays.asList("1", "2", "4", "3"),
                        Collections.singletonList("5"),
                        Collections.singletonList("6"));
    }

    private List<List<String>> toNames(List<SplitGenerator.SplitGroup> splitGroups) {
        return splitGroups.stream()
                .map(
                        splitGroup ->
                                splitGroup.files.stream()
                                        .map(DataFileMeta::fileName)
                                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
    }
}
