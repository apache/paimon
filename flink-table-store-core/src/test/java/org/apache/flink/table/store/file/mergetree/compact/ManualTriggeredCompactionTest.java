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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.SortedRun;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.mergetree.compact.CompactManagerTest.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ManualTriggeredCompaction}. */
public class ManualTriggeredCompactionTest {

    private final Comparator<RowData> comparator = Comparator.comparingInt(o -> o.getInt(0));
    private final ManualTriggeredCompaction strategy = new ManualTriggeredCompaction(10);
    private final int numLevels = 3;

    @Test
    public void testPickSmall() {
        assertThat(
                        strategy.pick(
                                numLevels,
                                generateNoOverlap(new int[] {0}, new int[] {1}, new int[] {2})))
                .isEmpty();

        assertThat(
                        strategy.pick(
                                numLevels,
                                generateNoOverlap(
                                        new int[] {0, 0}, new int[] {1, 5}, new int[] {2, 30})))
                .isEmpty();

        List<SortedRun> section =
                generateNoOverlap(new int[] {0, 0, 1}, new int[] {1, 5, 32}, new int[] {2, 30, 36});
        assertCompact(section);
    }

    @Test
    public void testPickOverlapped() {
        List<SortedRun> section =
                generateOverlap(
                        new int[][] {new int[] {0, 1, 20}, new int[] {0, 30, 50}},
                        new int[][] {new int[] {0, 16, 27}},
                        new int[][] {new int[] {1, 45, 47}, new int[] {2, 52, 60}});
        assertCompact(section);
    }

    private void assertCompact(List<SortedRun> section) {
        Optional<CompactUnit> actual = strategy.pick(numLevels, section);
        assertThat(actual).isPresent();
        assertThat(actual.get().outputLevel()).isEqualTo(numLevels - 1);
        assertThat(actual.get().files())
                .containsExactlyInAnyOrderElementsOf(
                        section.stream()
                                .map(SortedRun::files)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()));
    }

    private List<SortedRun> generateNoOverlap(int[] levels, int[] min, int[] max) {
        if (levels.length == 1) {
            return Collections.singletonList(
                    SortedRun.fromSingle(newFile(levels[0], min[0], max[0], max[0])));
        }
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < levels.length; i++) {
            files.add(newFile(levels[i], min[i], max[i], max[i]));
        }
        return Collections.singletonList(SortedRun.fromUnsorted(files, comparator));
    }

    private List<SortedRun> generateOverlap(int[][]... sequences) {
        List<SortedRun> runs = new ArrayList<>();
        for (int[][] sequence : sequences) {
            List<DataFileMeta> files = new ArrayList<>();
            for (int[] tuple : sequence) {
                files.add(newFile(tuple[0], tuple[1], tuple[2], tuple[2]));
            }
            runs.add(SortedRun.fromUnsorted(files, comparator));
        }
        return runs;
    }
}
