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
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;
import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.io.DataFileTestUtils.fromMinMax;
import static org.apache.paimon.io.DataFileTestUtils.newFile;
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
                0L,
                null);
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

    @Test
    public void testSplitRawConvertible() {
        Comparator<InternalRow> comparator = Comparator.comparingInt(o -> o.getInt(0));
        MergeTreeSplitGenerator mergeTreeSplitGenerator =
                new MergeTreeSplitGenerator(comparator, 100, 2, false, DEDUPLICATE);

        // When level0 exists, should not be rawConvertible
        List<DataFileMeta> files1 =
                Arrays.asList(newFile("1", 0, 0, 10, 10L), newFile("2", 0, 10, 20, 20L));
        assertThat(toNamesAndRawConvertible(mergeTreeSplitGenerator.splitForBatch(files1)))
                .containsExactlyInAnyOrder(Pair.of(Arrays.asList("1", "2"), false));

        // When deleteRowCount > 0, should not be rawConvertible
        List<DataFileMeta> files2 =
                Arrays.asList(newFile("1", 1, 0, 10, 10L, 1L), newFile("2", 1, 10, 20, 20L));
        assertThat(toNamesAndRawConvertible(mergeTreeSplitGenerator.splitForBatch(files2)))
                .containsExactlyInAnyOrder(Pair.of(Arrays.asList("1", "2"), false));

        // No level0 and deleteRowCount == 0:
        // All in one level, should be rawConvertible
        List<DataFileMeta> files3 =
                Arrays.asList(newFile("1", 1, 0, 10, 10L), newFile("2", 1, 10, 20, 20L));
        assertThat(toNamesAndRawConvertible(mergeTreeSplitGenerator.splitForBatch(files3)))
                .containsExactlyInAnyOrder(Pair.of(Arrays.asList("1", "2"), true));

        // Not all in one level, should not be rawConvertible
        List<DataFileMeta> files4 =
                Arrays.asList(newFile("1", 1, 0, 10, 10L), newFile("2", 2, 10, 20, 20L));
        assertThat(toNamesAndRawConvertible(mergeTreeSplitGenerator.splitForBatch(files4)))
                .containsExactlyInAnyOrder(Pair.of(Arrays.asList("1", "2"), false));

        // Not all in one level but with deletion vectors enabled, should be rawConvertible
        MergeTreeSplitGenerator splitGeneratorWithDVEnabled =
                new MergeTreeSplitGenerator(comparator, 100, 2, true, DEDUPLICATE);
        assertThat(toNamesAndRawConvertible(splitGeneratorWithDVEnabled.splitForBatch(files4)))
                .containsExactlyInAnyOrder(Pair.of(Arrays.asList("1", "2"), true));

        // Not all in one level but with first row merge engine, should be rawConvertible
        MergeTreeSplitGenerator splitGeneratorWithFirstRow =
                new MergeTreeSplitGenerator(comparator, 100, 2, false, FIRST_ROW);
        assertThat(toNamesAndRawConvertible(splitGeneratorWithFirstRow.splitForBatch(files4)))
                .containsExactlyInAnyOrder(Pair.of(Arrays.asList("1", "2"), true));

        // Split with one file should be rawConvertible
        List<DataFileMeta> files5 =
                Arrays.asList(
                        newFile("1", 1, 0, 10, 10L),
                        newFile("2", 2, 0, 12, 12L),
                        newFile("3", 3, 15, 60, 60L),
                        newFile("4", 4, 18, 40, 40L),
                        newFile("5", 5, 82, 85, 85L),
                        newFile("6", 6, 100, 200, 200L));
        assertThat(toNamesAndRawConvertible(mergeTreeSplitGenerator.splitForBatch(files5)))
                .containsExactlyInAnyOrder(
                        Pair.of(Arrays.asList("1", "2", "3", "4", "5"), false),
                        Pair.of(Collections.singletonList("6"), true));

        // test convertible for old version
        List<DataFileMeta> files6 =
                Arrays.asList(
                        newFile("1", 1, 0, 10, 10L, null), newFile("2", 1, 10, 20, 20L, null));
        assertThat(toNamesAndRawConvertible(mergeTreeSplitGenerator.splitForBatch(files6)))
                .containsExactlyInAnyOrder(Pair.of(Arrays.asList("1", "2"), true));
    }

    @Test
    public void testMergeTreeSplitRawConvertible() {
        Comparator<InternalRow> comparator = Comparator.comparingInt(o -> o.getInt(0));
        MergeTreeSplitGenerator mergeTreeSplitGenerator =
                new MergeTreeSplitGenerator(comparator, 100, 2, false, DEDUPLICATE);

        List<DataFileMeta> files =
                Arrays.asList(
                        newFile("1", 0, 0, 10, 10L),
                        newFile("2", 0, 0, 12, 12L),
                        newFile("3", 0, 13, 20, 20L),
                        newFile("4", 0, 21, 200, 200L),
                        newFile("5", 0, 201, 210, 210L),
                        newFile("6", 0, 211, 220, 220L));
        assertThat(toNamesAndRawConvertible(mergeTreeSplitGenerator.splitForBatch(files)))
                .containsExactlyInAnyOrder(
                        Pair.of(Arrays.asList("1", "2", "3"), false),
                        Pair.of(Collections.singletonList("4"), true),
                        Pair.of(Arrays.asList("5", "6"), false));
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

    private List<Pair<List<String>, Boolean>> toNamesAndRawConvertible(
            List<SplitGenerator.SplitGroup> splitGroups) {
        return splitGroups.stream()
                .map(
                        splitGroup -> {
                            List<String> sortedFileNames =
                                    splitGroup.files.stream()
                                            .sorted(Comparator.comparing(DataFileMeta::fileName))
                                            .map(DataFileMeta::fileName)
                                            .collect(Collectors.toList());
                            return Pair.of(sortedFileNames, splitGroup.rawConvertible);
                        })
                .collect(Collectors.toList());
    }
}
