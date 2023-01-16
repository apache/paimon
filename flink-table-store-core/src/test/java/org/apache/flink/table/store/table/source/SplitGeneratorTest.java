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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.io.DataFileMeta;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.io.DataFileTestUtils.fromMinMax;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AppendOnlySplitGenerator} and {@link MergeTreeSplitGenerator}. */
public class SplitGeneratorTest {

    private final List<DataFileMeta> files =
            Arrays.asList(
                    fromMinMax("1", 0, 10),
                    fromMinMax("2", 0, 12),
                    fromMinMax("3", 15, 60),
                    fromMinMax("4", 18, 40),
                    fromMinMax("5", 82, 85),
                    fromMinMax("6", 100, 200));

    @Test
    public void testAppend() {
        assertThat(toNames(new AppendOnlySplitGenerator(40, 2).split(files)))
                .containsExactlyInAnyOrder(
                        Arrays.asList("1", "2"),
                        Collections.singletonList("3"),
                        Arrays.asList("4", "5"),
                        Collections.singletonList("6"));

        assertThat(toNames(new AppendOnlySplitGenerator(70, 2).split(files)))
                .containsExactlyInAnyOrder(
                        Arrays.asList("1", "2", "3"),
                        Arrays.asList("4", "5"),
                        Collections.singletonList("6"));

        assertThat(toNames(new AppendOnlySplitGenerator(40, 20).split(files)))
                .containsExactlyInAnyOrder(
                        Arrays.asList("1", "2"),
                        Collections.singletonList("3"),
                        Collections.singletonList("4"),
                        Collections.singletonList("5"),
                        Collections.singletonList("6"));
    }

    @Test
    public void testMergeTree() {
        Comparator<InternalRow> comparator = Comparator.comparingInt(o -> o.getInt(0));
        assertThat(toNames(new MergeTreeSplitGenerator(comparator, 100, 2).split(files)))
                .containsExactlyInAnyOrder(
                        Arrays.asList("1", "2", "4", "3", "5"), Collections.singletonList("6"));

        assertThat(toNames(new MergeTreeSplitGenerator(comparator, 100, 30).split(files)))
                .containsExactlyInAnyOrder(
                        Arrays.asList("1", "2", "4", "3"),
                        Collections.singletonList("5"),
                        Collections.singletonList("6"));
    }

    private List<List<String>> toNames(List<List<DataFileMeta>> splits) {
        return splits.stream()
                .map(
                        files ->
                                files.stream()
                                        .map(DataFileMeta::fileName)
                                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
    }
}
