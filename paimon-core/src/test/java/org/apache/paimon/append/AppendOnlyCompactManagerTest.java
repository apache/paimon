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

package org.apache.paimon.append;

import org.apache.paimon.io.DataFileMeta;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AppendOnlyCompactManager}. */
public class AppendOnlyCompactManagerTest {

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

        // small file at last, release previous
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
                        newFile(2101L, 2110L)),
                false,
                Collections.emptyList(),
                Arrays.asList(newFile(2050L, 2100L), newFile(2101L, 2110L)));
        innerTest(
                Arrays.asList(newFile(1L, 1024L), newFile(1025L, 2049L), newFile(2050L, 2500L)),
                false,
                Collections.emptyList(),
                Collections.singletonList(newFile(2050L, 2500L)));
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
                Collections.singletonList(newFile(7001L, 7600L)));

        // ignore single small file (in the middle)
        innerTest(
                Arrays.asList(
                        newFile(1L, 1024L),
                        newFile(1025L, 2049L),
                        newFile(2050L, 2500L),
                        newFile(2501L, 4096L)),
                false,
                Collections.emptyList(),
                Collections.singletonList(newFile(2501L, 4096L)));

        innerTest(
                Arrays.asList(
                        newFile(1L, 1024L),
                        newFile(1025L, 2049L),
                        newFile(2050L, 2500L),
                        newFile(2501L, 4096L),
                        newFile(4097L, 6000L)),
                false,
                Collections.emptyList(),
                Collections.singletonList(newFile(4097L, 6000L)));

        // wait for more file
        innerTest(
                Arrays.asList(newFile(1L, 500L), newFile(501L, 1000L)),
                false,
                Collections.emptyList(),
                Arrays.asList(newFile(1L, 500L), newFile(501L, 1000L)));

        innerTest(
                Arrays.asList(newFile(1L, 500L), newFile(501L, 1000L), newFile(1001L, 2026L)),
                false,
                Collections.emptyList(),
                Arrays.asList(newFile(501L, 1000L), newFile(1001L, 2026L)));

        innerTest(
                Arrays.asList(newFile(1L, 2000L), newFile(2001L, 2005L), newFile(2006L, 2010L)),
                false,
                Collections.emptyList(),
                Arrays.asList(newFile(2001L, 2005L), newFile(2006L, 2010L)));
    }

    @Test
    public void testPick() {
        // fileNum is 13 (which > 12) and totalFileSize is 130 (which < 1024)
        List<DataFileMeta> toCompact1 =
                Arrays.asList(
                        // 1~10, 11~20, ..., 111~120
                        newFile(1L, 10L),
                        newFile(11L, 20L),
                        newFile(21L, 30L),
                        newFile(31L, 40L),
                        newFile(41L, 50L),
                        newFile(51L, 60L),
                        newFile(61L, 70L),
                        newFile(71L, 80L),
                        newFile(81L, 90L),
                        newFile(91L, 100L),
                        newFile(101L, 110L),
                        newFile(111L, 120L),
                        newFile(121L, 130L));
        innerTest(
                toCompact1,
                true,
                toCompact1.subList(0, toCompact1.size() - 1),
                Collections.singletonList(newFile(121L, 130L)));

        // fileNum is 4 (which > 3) and totalFileSize is 1026 (which > 1024)
        List<DataFileMeta> toCompact2 =
                Arrays.asList(
                        // 1~2, 3~500, 501~1000, 1001~1025
                        newFile(1L, 2L),
                        newFile(3L, 500L),
                        newFile(501L, 1000L),
                        newFile(1001L, 1025L),
                        newFile(1026L, 1050L));
        innerTest(
                toCompact2,
                true,
                toCompact2.subList(0, 4),
                Collections.singletonList(newFile(1026L, 1050L)));

        // fileNum is 13 (which > 12) and totalFileSize is 130 (which < 1024)
        List<DataFileMeta> toCompact3 =
                Arrays.asList(
                        newFile(1L, 1022L),
                        newFile(1023L, 1024L),
                        newFile(1025L, 2050L),
                        // 2051~2510, ..., 2611~2620
                        newFile(2051L, 2510L),
                        newFile(2511L, 2520L),
                        newFile(2521L, 2530L),
                        newFile(2531L, 2540L),
                        newFile(2541L, 2550L),
                        newFile(2551L, 2560L),
                        newFile(2561L, 2570L),
                        newFile(2571L, 2580L),
                        newFile(2581L, 2590L),
                        newFile(2591L, 2600L),
                        newFile(2601L, 2610L),
                        newFile(2611L, 2620L),
                        newFile(2621L, 2630L));
        innerTest(
                toCompact3,
                true,
                toCompact3.subList(3, toCompact3.size() - 1),
                Collections.singletonList(newFile(2621L, 2630L)));
    }

    private void innerTest(
            List<DataFileMeta> toCompactBeforePick,
            boolean expectedPresent,
            List<DataFileMeta> expectedCompactBefore,
            List<DataFileMeta> toCompactAfterPick) {
        int minFileNum = 4;
        int maxFileNum = 12;
        long targetFileSize = 1024;
        AppendOnlyCompactManager manager =
                new AppendOnlyCompactManager(
                        null, // not used
                        toCompactBeforePick,
                        null,
                        minFileNum,
                        maxFileNum,
                        targetFileSize,
                        null, // not used
                        null);
        Optional<List<DataFileMeta>> actual = manager.pickCompactBefore();
        assertThat(actual.isPresent()).isEqualTo(expectedPresent);
        if (expectedPresent) {
            assertThat(actual.get()).containsExactlyElementsOf(expectedCompactBefore);
        }
        assertThat(manager.getToCompact()).containsExactlyElementsOf(toCompactAfterPick);
    }
}
