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

package org.apache.flink.table.store.file.data;

import org.apache.flink.table.store.file.compact.CompactUnit;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.store.file.data.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AppendOnlyCompactStrategy}. */
public class AppendOnlyCompactStrategyTest {

    private static final long TARGET_FILE_SIZE = 20L;

    private final AppendOnlyCompactStrategy strategy =
            new AppendOnlyCompactStrategy(TARGET_FILE_SIZE);

    @Test
    public void testPickSingle() {
        assertThat(strategy.pick(Collections.singletonList(newFile(1L, 5L)))).isEmpty();
        assertThat(strategy.pick(Collections.singletonList(newFile(1L, 21L)))).isEmpty();
    }

    @Test
    public void testPickSingleSmall() {
        innerTest(
                Arrays.asList(newFile(1L, 5L), newFile(6L, 30L)),
                Collections.singletonList(Arrays.asList(newFile(1L, 5L), newFile(6L, 30L))));

        innerTest(
                Arrays.asList(newFile(1L, 20L), newFile(21L, 30L)),
                Collections.singletonList(Arrays.asList(newFile(1L, 20L), newFile(21L, 30L))));
    }

    @Test
    public void testPickContinuousSmall() {
        innerTest(
                Arrays.asList(
                        newFile(1L, 5L),
                        newFile(6L, 10L),
                        newFile(11L, 30L),
                        newFile(31L, 33L),
                        newFile(34L, 40L)),
                Arrays.asList(
                        Arrays.asList(newFile(1L, 5L), newFile(6L, 10L)),
                        Arrays.asList(newFile(31L, 33L), newFile(34L, 40L))));

        innerTest(
                Arrays.asList(newFile(1L, 10L), newFile(11L, 20L), newFile(21L, 25L)),
                Collections.singletonList(
                        Arrays.asList(newFile(1L, 10L), newFile(11L, 20L), newFile(21L, 25L))));
    }

    @Test
    public void testPickComplex() {
        innerTest(
                Arrays.asList(
                        newFile(1L, 20L),
                        newFile(21L, 25L),
                        newFile(26L, 50L),
                        newFile(51L, 55L),
                        newFile(56L, 60L),
                        newFile(61L, 80L),
                        newFile(81L, 85L)),
                Arrays.asList(
                        Arrays.asList(newFile(21L, 25L), newFile(26L, 50L)),
                        Arrays.asList(newFile(51L, 55L), newFile(56L, 60L)),
                        Arrays.asList(newFile(61L, 80L), newFile(81L, 85L))));

        innerTest(
                Arrays.asList(
                        newFile(1L, 20L),
                        newFile(21L, 25L),
                        newFile(26L, 50L),
                        newFile(51L, 55L),
                        newFile(56L, 60L),
                        newFile(61L, 69L)),
                Arrays.asList(
                        Arrays.asList(newFile(21L, 25L), newFile(26L, 50L)),
                        Arrays.asList(newFile(51L, 55L), newFile(56L, 60L), newFile(61L, 69L))));
    }

    private void innerTest(List<DataFileMeta> newFiles, List<List<DataFileMeta>> expected) {
        Optional<List<CompactUnit>> optional = strategy.pick(newFiles);
        assertThat(optional).isPresent();
        List<CompactUnit> units = optional.get();
        for (int i = 0; i < units.size(); i++) {
            assertThat(units.get(i).files()).containsExactlyInAnyOrderElementsOf(expected.get(i));
        }
    }
}
