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

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for {@link IterativeCompactTaskTest}. */
public class IterativeCompactTaskTest {

    private static final long TARGET_FILE_SIZE = 1024L;
    private static final int MIN_FILE_NUM = 3;
    private static final int MAX_FILE_NUM = 10;

    @Test
    public void testNoCompact() {
        // empty
        innerTest(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());

        // single small file
        innerTest(
                Collections.singletonList(newFile(1L, 10L)),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());

        // single large file
        innerTest(
                Collections.singletonList(newFile(1L, 1024L)),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());

        // almost-full files
        innerTest(
                Arrays.asList(newFile(1L, 1024L), newFile(1025L, 2048L)),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());

        // large files
        innerTest(
                Arrays.asList(newFile(1L, 1000L), newFile(1001L, 1100L)),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
    }

    @Test
    public void testCompactOnce() {
        // small files on the head
        innerTest(
                Arrays.asList(
                        newFile(1L, 500L),
                        newFile(501L, 1000L),
                        newFile(1001L, 1010L),
                        newFile(1011L, 1024L),
                        newFile(1025L, 3000L)),
                Arrays.asList(
                        newFile(1L, 500L),
                        newFile(501L, 1000L),
                        newFile(1001L, 1010L),
                        newFile(1011L, 1024L)),
                Collections.singletonList(newFile(1L, 1024L)),
                Collections.emptyList());

        innerTest(
                Arrays.asList(
                        newFile(1L, 500L),
                        newFile(501L, 1000L),
                        newFile(1001L, 1010L),
                        newFile(1011L, 2000L),
                        newFile(2001L, 3000L)),
                Arrays.asList(
                        newFile(1L, 500L),
                        newFile(501L, 1000L),
                        newFile(1001L, 1010L),
                        newFile(1011L, 2000L)),
                Arrays.asList(newFile(1L, 1024L), newFile(1025L, 2000L)),
                Collections.emptyList());

        // small files in the middle
        innerTest(
                Arrays.asList(
                        newFile(1L, 2000L),
                        newFile(2001L, 4000L),
                        newFile(4001L, 4500L),
                        newFile(4501L, 4600L),
                        newFile(4601L, 4700L),
                        newFile(4701L, 5024L),
                        newFile(5025L, 7000L)),
                Arrays.asList(
                        newFile(4001L, 4500L),
                        newFile(4501L, 4600L),
                        newFile(4601L, 4700L),
                        newFile(4701L, 5024L)),
                Collections.singletonList(newFile(4001L, 5024L)),
                Collections.emptyList());

        // small files on the tail
        innerTest(
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
                        newFile(4091L, 4110L)),
                Arrays.asList(
                        newFile(4001L, 4010L),
                        newFile(4011L, 4020L),
                        newFile(4021L, 4030L),
                        newFile(4031L, 4040L),
                        newFile(4041L, 4050L),
                        newFile(4051L, 4060L),
                        newFile(4061L, 4070L),
                        newFile(4071L, 4080L),
                        newFile(4081L, 4090L),
                        newFile(4091L, 4110L)),
                Collections.singletonList(newFile(4001L, 4110L)),
                Collections.emptyList());
    }

    @Test
    public void testCompactMultiple() {
        // continuous compact
        innerTest(
                Arrays.asList(
                        newFile(1L, 2000L),
                        newFile(2001L, 4000L),
                        // 4001~4010, ..., 4091~4110
                        newFile(4001L, 4010L),
                        newFile(4011L, 4020L),
                        newFile(4021L, 4030L),
                        newFile(4031L, 4040L),
                        newFile(4041L, 4050L),
                        newFile(4051L, 4060L),
                        newFile(4061L, 4070L),
                        newFile(4071L, 4080L),
                        newFile(4081L, 4090L),
                        newFile(4091L, 4110L),
                        // 4001~4110, 5015~5024
                        newFile(4111L, 5000L),
                        newFile(5001L, 5014L),
                        newFile(5015L, 5024L)),
                Arrays.asList(
                        newFile(4001L, 4010L),
                        newFile(4011L, 4020L),
                        newFile(4021L, 4030L),
                        newFile(4031L, 4040L),
                        newFile(4041L, 4050L),
                        newFile(4051L, 4060L),
                        newFile(4061L, 4070L),
                        newFile(4071L, 4080L),
                        newFile(4081L, 4090L),
                        newFile(4091L, 4110L),
                        newFile(4111L, 5000L),
                        newFile(5001L, 5014L),
                        newFile(5015L, 5024L)),
                Collections.singletonList(newFile(4001L, 5024L)),
                Collections.singletonList(newFile(4001L, 4110L)));

        // alternate compact
        innerTest(
                Arrays.asList(
                        newFile(1L, 2000L),
                        newFile(2001L, 4000L),
                        // 4001~4500, ..., 4701~6000
                        newFile(4001L, 4500L),
                        newFile(4501L, 4600L),
                        newFile(4601L, 4700L),
                        newFile(4701L, 6000L),
                        newFile(6001L, 7500L),
                        // 7501~8000, 8201~8900
                        newFile(7501L, 8000L),
                        newFile(8001L, 8200L),
                        newFile(8201L, 8900L),
                        newFile(8901L, 9550L)),
                Arrays.asList(
                        newFile(4001L, 4500L),
                        newFile(4501L, 4600L),
                        newFile(4601L, 4700L),
                        newFile(4701L, 6000L),
                        newFile(7501L, 8000L),
                        newFile(8001L, 8200L),
                        newFile(8201L, 8900L)),
                Arrays.asList(
                        newFile(4001L, 5024L),
                        newFile(5025L, 6000L),
                        newFile(7501L, 8524L),
                        newFile(8525L, 8900L)),
                Collections.emptyList());
    }

    private void innerTest(
            List<DataFileMeta> compactFiles,
            List<DataFileMeta> expectBefore,
            List<DataFileMeta> expectAfter,
            List<DataFileMeta> expectDeleted) {
        MockIterativeCompactTask task =
                new MockIterativeCompactTask(
                        compactFiles, TARGET_FILE_SIZE, MIN_FILE_NUM, MAX_FILE_NUM, rewriter());
        try {
            CompactResult actual = task.doCompact();
            assertThat(actual.before()).containsExactlyInAnyOrderElementsOf(expectBefore);
            assertThat(actual.after()).containsExactlyInAnyOrderElementsOf(expectAfter);

            // assert the temporary files are deleted
            assertThat(task.deleted).containsExactlyInAnyOrderElementsOf(expectDeleted);
        } catch (Exception e) {
            fail("This should not happen", e);
        }
    }

    /** A Mock {@link AppendOnlyCompactManager.IterativeCompactTask} to test. */
    private static class MockIterativeCompactTask
            extends AppendOnlyCompactManager.IterativeCompactTask {

        private final Set<DataFileMeta> deleted;

        public MockIterativeCompactTask(
                List<DataFileMeta> inputs,
                long targetFileSize,
                int minFileNum,
                int maxFileNum,
                AppendOnlyCompactManager.CompactRewriter rewriter) {
            super(
                    LocalFileIO.create(),
                    inputs,
                    targetFileSize,
                    minFileNum,
                    maxFileNum,
                    rewriter,
                    null,
                    false);
            deleted = new HashSet<>();
        }

        @Override
        void delete(DataFileMeta tmpFile) {
            deleted.add(tmpFile);
        }
    }

    private AppendOnlyCompactManager.CompactRewriter rewriter() {
        return compactBefore -> {
            List<DataFileMeta> compactAfter = new ArrayList<>();
            long totalFileSize = 0L;
            long minSeq = -1L;
            for (int i = 0; i < compactBefore.size(); i++) {
                DataFileMeta file = compactBefore.get(i);
                if (i == 0) {
                    minSeq = file.minSequenceNumber();
                }
                totalFileSize += file.fileSize();
                if (totalFileSize >= TARGET_FILE_SIZE) {
                    compactAfter.add(newFile(minSeq, minSeq + TARGET_FILE_SIZE - 1));
                    minSeq += TARGET_FILE_SIZE;
                }
                if (i == compactBefore.size() - 1 && minSeq <= file.maxSequenceNumber()) {
                    compactAfter.add(newFile(minSeq, file.maxSequenceNumber()));
                }
            }
            return compactAfter;
        };
    }
}
