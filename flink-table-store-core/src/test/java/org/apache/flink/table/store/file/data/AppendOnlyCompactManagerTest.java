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

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.table.store.file.data.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AppendOnlyCompactManager}. */
public class AppendOnlyCompactManagerTest {

    private static ExecutorService service;

    @BeforeAll
    public static void before() {
        service = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    public static void after() {
        service.shutdownNow();
        service = null;
    }

    @Test
    public void testFindSmallFileIntervals() {
        // empty
        assertIntervals(Collections.singletonList(newFile(1L, 100L)), Collections.emptyList());
        assertIntervals(
                Arrays.asList(newFile(1L, 200L), newFile(201L, 300L)), Collections.emptyList());
        assertIntervals(
                Arrays.asList(newFile(1L, 200L), newFile(201L, 300L), newFile(301L, 450L)),
                Collections.emptyList());

        // single small file
        assertIntervals(
                Collections.singletonList(newFile(1L, 50L)),
                Collections.singletonList(Tuple2.of(0, 0)));
        assertIntervals(
                Arrays.asList(
                        newFile(1L, 101L),
                        newFile(102L, 200L),
                        newFile(201L, 305L),
                        newFile(306L, 400L)),
                Arrays.asList(Tuple2.of(1, 1), Tuple2.of(3, 3)));

        // adjacent small files
        assertIntervals(
                Arrays.asList(
                        newFile(1L, 101L),
                        newFile(102L, 200L),
                        newFile(201L, 205L),
                        newFile(206L, 300L)),
                Collections.singletonList(Tuple2.of(1, 3)));
        assertIntervals(
                Arrays.asList(
                        newFile(1L, 101L),
                        newFile(102L, 200L),
                        newFile(201L, 205L),
                        newFile(206L, 300L),
                        newFile(301L, 400L),
                        newFile(401L, 450L),
                        newFile(451L, 500L)),
                Arrays.asList(Tuple2.of(1, 3), Tuple2.of(5, 6)));

        // adjacent small files mix single small file
        assertIntervals(
                Arrays.asList(
                        newFile(1L, 50L),
                        newFile(51L, 60L),
                        newFile(61L, 100L),
                        newFile(101L, 300L),
                        newFile(301L, 310L)),
                Arrays.asList(Tuple2.of(0, 2), Tuple2.of(4, 4)));
        assertIntervals(
                Arrays.asList(
                        newFile(1L, 100L),
                        newFile(101L, 160L),
                        newFile(161L, 200L),
                        newFile(201L, 250L),
                        newFile(251L, 400L),
                        newFile(401L, 430L)),
                Arrays.asList(Tuple2.of(1, 3), Tuple2.of(5, 5)));
        assertIntervals(
                Arrays.asList(
                        newFile(1L, 100L),
                        newFile(101L, 160L),
                        newFile(161L, 200L),
                        newFile(201L, 350L),
                        newFile(351, 400L),
                        newFile(401L, 450L),
                        newFile(451L, 500L),
                        newFile(501L, 700L),
                        newFile(701L, 850L),
                        newFile(851L, 900L)),
                Arrays.asList(Tuple2.of(1, 2), Tuple2.of(4, 6), Tuple2.of(9, 9)));
    }

    @Test
    public void testPickEmptyAndNotRelease() {
        // 1~50 is small enough, so hold it
        List<DataFileMeta> toCompact = Collections.singletonList(newFile(1L, 50L));
        innerTest(toCompact, false, Collections.emptyList(), toCompact);
    }

    @Test
    public void testPickEmptyAndRelease() {
        // large file, release
        innerTest(
                Collections.singletonList(newFile(1L, 1024L)),
                false,
                Collections.emptyList(),
                Collections.emptyList());

        // small file at last and small enough, release previous
        innerTest(
                Arrays.asList(newFile(1L, 1024L), newFile(1025L, 2049L), newFile(2050L, 2100L)),
                false,
                Collections.emptyList(),
                Collections.singletonList(newFile(2050L, 2100L)));
        innerTest(
                Arrays.asList(
                        newFile(1L, 1024L),
                        newFile(1025L, 2049L),
                        newFile(2050L, 2100L),
                        newFile(2100L, 2110L)),
                false,
                Collections.emptyList(),
                Arrays.asList(newFile(2050L, 2100L), newFile(2100L, 2110L)));

        // small file at last (but not small enough), release all
        innerTest(
                Arrays.asList(newFile(1L, 1024L), newFile(1025L, 2049L), newFile(2050L, 2500L)),
                false,
                Collections.emptyList(),
                Collections.emptyList());
        innerTest(
                Arrays.asList(
                        newFile(1L, 1024L),
                        newFile(1025L, 2049L),
                        newFile(2050L, 2500L),
                        newFile(2501L, 4096L),
                        newFile(4097L, 6000L),
                        newFile(6001L, 7000L),
                        newFile(7001L, 7600L)),
                false,
                Collections.emptyList(),
                Collections.emptyList());

        // ignore single small file (in the middle) and release all
        innerTest(
                Arrays.asList(
                        newFile(1L, 1024L),
                        newFile(1025L, 2049L),
                        newFile(2050L, 2500L),
                        newFile(2501L, 4096L)),
                false,
                Collections.emptyList(),
                Collections.emptyList());

        innerTest(
                Arrays.asList(
                        newFile(1L, 1024L),
                        newFile(1025L, 2049L),
                        newFile(2050L, 2500L),
                        newFile(2501L, 4096L),
                        newFile(4097L, 6000L)),
                false,
                Collections.emptyList(),
                Collections.emptyList());

        // totalFileSize is 1000 (which > 1024 / 4), and is not cost-effective
        innerTest(
                Arrays.asList(newFile(1L, 500L), newFile(501L, 1000L)),
                false,
                Collections.emptyList(),
                Collections.emptyList());

        innerTest(
                Arrays.asList(newFile(1L, 500L), newFile(501L, 1000L), newFile(1001L, 2026L)),
                false,
                Collections.emptyList(),
                Collections.emptyList());

        // totalFileSize is 10 (which < 1024 / 4, but not meet the fileNumTrigger)
        innerTest(
                Arrays.asList(newFile(1L, 2000L), newFile(2001L, 2005L), newFile(2006L, 2010L)),
                false,
                Collections.emptyList(),
                Arrays.asList(newFile(2001L, 2005L), newFile(2006L, 2010L)));
    }

    @Test
    public void testPickAndNotRelease() {
        // fileNum is 10 (which > 8) and totalFileSize is 100 (which < 1024 / 4)
        List<DataFileMeta> toCompact1 =
                Arrays.asList(
                        newFile(1L, 10L),
                        newFile(11L, 20L),
                        newFile(21L, 30L),
                        newFile(31L, 40L),
                        newFile(41L, 50L),
                        newFile(51L, 60L),
                        newFile(61L, 70L),
                        newFile(71L, 80L),
                        newFile(81L, 90L),
                        newFile(91L, 100L));
        innerTest(toCompact1, true, toCompact1, toCompact1);

        // fileNum is 9 (which > 8) and totalFileSize is 90 (which < 1024 / 4)
        List<DataFileMeta> toCompact2 =
                Arrays.asList(
                        newFile(1L, 10L),
                        newFile(11L, 20L),
                        newFile(21L, 30L),
                        newFile(31L, 40L),
                        newFile(41L, 50L),
                        newFile(51L, 60L),
                        newFile(61L, 70L),
                        newFile(71L, 80L),
                        newFile(81L, 90L),
                        newFile(91L, 2000L),
                        newFile(2001L, 2200L));
        innerTest(toCompact2, true, toCompact2.subList(0, 9), toCompact2);
    }

    @Test
    public void testPickAndRelease() {
        List<DataFileMeta> toCompact1 =
                Arrays.asList(
                        newFile(1L, 2000L),
                        newFile(2001L, 4000L),
                        newFile(4001L, 4010L),
                        newFile(4011L, 4020L),
                        newFile(4021L, 4030L),
                        newFile(4031L, 4040L),
                        newFile(4041L, 4050L),
                        newFile(4051L, 4060L),
                        newFile(4061L, 4070L),
                        newFile(4071L, 4080L),
                        newFile(4081L, 4090L),
                        newFile(4091L, 4100L));
        innerTest(toCompact1, true, toCompact1.subList(2, 12), toCompact1.subList(2, 12));

        List<DataFileMeta> toCompact2 =
                Arrays.asList(
                        newFile(1L, 2000L),
                        // 2001~2010, 2011~2500 are skipped
                        newFile(2001L, 2010L),
                        newFile(2011L, 2500L),
                        newFile(2501L, 4000L),
                        // 4001~4010 are skipped
                        newFile(4001L, 4010L),
                        newFile(4011L, 6000L),
                        // 6001~6010, ..., 6051~6080 are picked
                        newFile(6001L, 6010L),
                        newFile(6011L, 6020L),
                        newFile(6021L, 6030L),
                        newFile(6031L, 6040L),
                        newFile(6041L, 6050L),
                        newFile(6051L, 6060L),
                        newFile(6061L, 6070L),
                        newFile(6071L, 6080L),
                        newFile(6081L, 6090L),
                        newFile(6091L, 8000L));
        innerTest(toCompact2, true, toCompact2.subList(6, 15), toCompact2.subList(6, 16));
    }

    private void innerTest(
            List<DataFileMeta> toCompactBeforePick,
            boolean expectedPresent,
            List<DataFileMeta> expectedCompactBefore,
            List<DataFileMeta> toCompactAfterPick) {
        int fileNumTrigger = 8;
        int fileSizeRatioTrigger = 4;
        long targetFileSize = 1024;
        AppendOnlyCompactManager compactManager =
                new AppendOnlyCompactManager(
                        service,
                        new LinkedList<>(toCompactBeforePick),
                        fileNumTrigger,
                        fileSizeRatioTrigger,
                        targetFileSize,
                        null);
        Optional<List<DataFileMeta>> actual = compactManager.pickCompactBefore();
        assertThat(actual.isPresent()).isEqualTo(expectedPresent);
        if (expectedPresent) {
            assertThat(actual.get()).containsExactlyElementsOf(expectedCompactBefore);
        }
        assertThat(compactManager.getToCompact()).containsExactlyElementsOf(toCompactAfterPick);
    }

    private void assertIntervals(
            List<DataFileMeta> toCompact, List<Tuple2<Integer, Integer>> expected) {
        assertThat(AppendOnlyCompactManager.findSmallFileIntervals(toCompact, 100L))
                .containsExactlyInAnyOrderElementsOf(expected);
    }
}
