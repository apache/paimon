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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.stats.StatsTestUtils;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.stats.StatsTestUtils.newTableStats;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IntervalPartition}. */
public class IntervalPartitionTest {

    private static final RecordComparator COMPARATOR =
            (RecordComparator) (o1, o2) -> o1.getInt(0) - o2.getInt(0);

    @Test
    public void testSameMinKey() {
        runTest(
                "[100, 200], [100, 400], [100, 300], [100, 500]",
                "[100, 200] | [100, 300] | [100, 400] | [100, 500]");
    }

    @Test
    public void testSameMaxKey() {
        runTest(
                "[100, 500], [300, 500], [200, 500], [400, 500]",
                "[100, 500] | [200, 500] | [300, 500] | [400, 500]");
    }

    @Test
    public void testSectionPartitioning() {
        // 0    5    10   15   20   25   30
        // |--------|
        //      |-|
        //          |-----|
        //                 |-----|
        //                 |-----------|
        //                         |-------|
        // 0    5    10   15   20   25   30
        runTest(
                "[0, 9], [5, 7], [9, 15], [16, 22], [16, 28], [24, 32]",
                "[0, 9] | [5, 7], [9, 15]\n" + "[16, 22], [24, 32] | [16, 28]");
    }

    private void runTest(String in, String ans) {
        IntervalPartition algorithm = new IntervalPartition(parseMetas(in), COMPARATOR);
        List<List<SortedRun>> expected = new ArrayList<>();
        for (String line : ans.split("\n")) {
            expected.add(parseSortedRuns(line));
        }

        List<List<SortedRun>> actual = algorithm.partition();
        for (List<SortedRun> section : actual) {
            for (SortedRun sortedRun : section) {
                sortedRun.validate(COMPARATOR);
            }
        }

        // compare the results with multiset because the order between sorted runs within a section
        // does not matter
        assertThat(toMultiset(actual)).isEqualTo(toMultiset(expected));
    }

    @RepeatedTest(100)
    public void randomTest() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        List<int[]> intervals = new ArrayList<>();
        // construct some sorted runs
        int numSortedRuns = r.nextInt(10) + 1;
        for (int i = 0; i < numSortedRuns; i++) {
            int numIntervals = r.nextInt(10) + 1;
            // pick 2 * numIntervals distinct integers to make intervals
            Set<Integer> set = new TreeSet<>();
            while (set.size() < 2 * numIntervals) {
                int x;
                do {
                    x = r.nextInt(1000);
                } while (set.contains(x));
                set.add(x);
            }
            List<Integer> ints = new ArrayList<>(set);
            for (int j = 0; j < 2 * numIntervals; j += 2) {
                intervals.add(new int[] {ints.get(j), ints.get(j + 1)});
            }
        }
        // change the input to string
        String input =
                intervals.stream()
                        .map(a -> String.format("[%d, %d]", a[0], a[1]))
                        .collect(Collectors.joining(", "));
        // maximum number of sorted runs after partitioning must not exceed numSortedRuns
        IntervalPartition algorithm = new IntervalPartition(parseMetas(input), COMPARATOR);
        List<List<SortedRun>> result = algorithm.partition();
        for (List<SortedRun> section : result) {
            assertThat(section.size() <= numSortedRuns).isTrue();
            for (SortedRun sortedRun : section) {
                sortedRun.validate(COMPARATOR);
            }
        }
    }

    private List<SortedRun> parseSortedRuns(String in) {
        List<SortedRun> sortedRuns = new ArrayList<>();
        for (String s : in.split("\\|")) {
            sortedRuns.add(SortedRun.fromSorted(parseMetas(s)));
        }
        return sortedRuns;
    }

    private List<DataFileMeta> parseMetas(String in) {
        List<DataFileMeta> metas = new ArrayList<>();
        Pattern pattern = Pattern.compile("\\[(\\d+?), (\\d+?)]");
        Matcher matcher = pattern.matcher(in);
        while (matcher.find()) {
            metas.add(
                    makeInterval(
                            Integer.parseInt(matcher.group(1)),
                            Integer.parseInt(matcher.group(2))));
        }
        return metas;
    }

    private DataFileMeta makeInterval(int left, int right) {
        BinaryRow minKey = new BinaryRow(1);
        BinaryRowWriter minWriter = new BinaryRowWriter(minKey);
        minWriter.writeInt(0, left);
        minWriter.complete();
        BinaryRow maxKey = new BinaryRow(1);
        BinaryRowWriter maxWriter = new BinaryRowWriter(maxKey);
        maxWriter.writeInt(0, right);
        maxWriter.complete();

        return new DataFileMeta(
                "DUMMY",
                100,
                25,
                minKey,
                maxKey,
                newTableStats(left, right),
                StatsTestUtils.newEmptyTableStats(), // not used
                0,
                24,
                0,
                0,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(100000),
                0L,
                null,
                null);
    }

    private List<Map<SortedRun, Integer>> toMultiset(List<List<SortedRun>> sections) {
        List<Map<SortedRun, Integer>> result = new ArrayList<>();
        for (List<SortedRun> section : sections) {
            Map<SortedRun, Integer> multiset = new HashMap<>();
            for (SortedRun sortedRun : section) {
                multiset.compute(sortedRun, (k, v) -> v == null ? 1 : v + 1);
            }
            result.add(multiset);
        }
        return result;
    }
}
