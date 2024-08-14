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
import org.apache.paimon.io.DataFileMeta;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Test for {@link FullCompactTaskTest}. */
public class FullCompactTaskTest {

    private static final long TARGET_FILE_SIZE = 1024L;

    @Test
    public void testNoCompact() {
        // empty
        innerTest(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        // single small file
        innerTest(
                Collections.singletonList(newFile(1L, 10L)),
                Collections.emptyList(),
                Collections.emptyList());

        // single large file
        innerTest(
                Collections.singletonList(newFile(1L, 1024L)),
                Collections.emptyList(),
                Collections.emptyList());

        // large files
        innerTest(
                Arrays.asList(newFile(1L, 1024L), newFile(1025L, 2048L), newFile(2049, 3200)),
                Collections.emptyList(),
                Collections.emptyList());

        // large files more than small files
        innerTest(
                Arrays.asList(
                        newFile(1L, 1024L),
                        newFile(1025L, 2048L),
                        newFile(2049, 3200),
                        newFile(3201, 3220),
                        newFile(3221, 3280)),
                Collections.emptyList(),
                Collections.emptyList());
    }

    @Test
    public void testCompact() {
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
                        newFile(1011L, 1024L),
                        newFile(1025L, 3000L)),
                Arrays.asList(newFile(1L, 1024L), newFile(1025L, 2048L), newFile(2049, 3000)));

        // no head
        innerTest(
                Arrays.asList(
                        newFile(1L, 1025),
                        newFile(1026, 1080),
                        newFile(1090, 1500),
                        newFile(1600, 1900)),
                Arrays.asList(newFile(1026, 1080), newFile(1090, 1500), newFile(1600, 1900)),
                Collections.singletonList(newFile(1026, 1900)));
    }

    private void innerTest(
            List<DataFileMeta> compactFiles,
            List<DataFileMeta> expectBefore,
            List<DataFileMeta> expectAfter) {
        MockFullCompactTask task =
                new MockFullCompactTask(compactFiles, TARGET_FILE_SIZE, rewriter());
        try {
            CompactResult actual = task.doCompact();
            assertThat(actual.before()).containsExactlyInAnyOrderElementsOf(expectBefore);
            assertThat(actual.after()).containsExactlyInAnyOrderElementsOf(expectAfter);
        } catch (Exception e) {
            fail("This should not happen", e);
        }
    }

    /** A Mock {@link BucketedAppendCompactManager.FullCompactTask} to test. */
    private static class MockFullCompactTask extends BucketedAppendCompactManager.FullCompactTask {

        public MockFullCompactTask(
                Collection<DataFileMeta> inputs,
                long targetFileSize,
                BucketedAppendCompactManager.CompactRewriter rewriter) {
            super(inputs, targetFileSize, rewriter, null);
        }
    }

    private BucketedAppendCompactManager.CompactRewriter rewriter() {
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
