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

package org.apache.paimon.mergetree;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.PojoDataFileMeta;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the fix to Issue #5872: ensure deterministic file ordering when files have the same
 * maxSequenceNumber to prevent data corruption during compaction.
 */
public class LevelsOrderingFixTest {

    @Test
    public void testIssue5872FixDeterministicOrdering() {
        String filename1 = "z-file.parquet";
        String filename2 = "a-file.parquet";

        DataFileMeta earlierFile = createTestFile(filename1, 98L, 100L, 1000L);
        DataFileMeta laterFile = createTestFile(filename2, 99L, 100L, 1001L);

        List<DataFileMeta> files = new ArrayList<>();
        files.add(earlierFile);
        files.add(laterFile);

        Levels levels = new Levels(null, files, 2);
        List<DataFileMeta> orderedFiles = new ArrayList<>(levels.allFiles());

        for (int i = 0; i < orderedFiles.size(); i++) {
            DataFileMeta f = orderedFiles.get(i);
        }

        String actualFirstFile = orderedFiles.get(0).fileName();
        long actualFirstMinSeq = orderedFiles.get(0).minSequenceNumber();

        assertThat(orderedFiles.get(0).fileName())
                .as("correct sequence")
                .isEqualTo("z-file.parquet");

        assertThat(orderedFiles.get(0).minSequenceNumber())
                .as("File with earlier minSequenceNumber should be processed first")
                .isEqualTo(98L);

        List<DataFileMeta> filesReversed = new ArrayList<>();
        filesReversed.add(laterFile);
        filesReversed.add(earlierFile);

        Levels levelsReversed = new Levels(null, filesReversed, 2);
        List<DataFileMeta> orderedFilesReversed = new ArrayList<>(levelsReversed.allFiles());

        assertThat(orderedFilesReversed)
                .as("Order should be independent of insertion sequence")
                .isEqualTo(orderedFiles);
    }

    private String generateRandomFilename(Random random) {
        String[] patterns = {"data-%03d.parquet", "file-%s.parquet", "part-%03d.parquet"};
        String pattern = patterns[random.nextInt(patterns.length)];

        if (pattern.contains("%03d")) {
            return String.format(pattern, random.nextInt(1000));
        } else {
            String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 6; i++) {
                sb.append(chars.charAt(random.nextInt(chars.length())));
            }
            return String.format(pattern, sb.toString());
        }
    }

    private DataFileMeta createTestFile(
            String fileName, long minSeq, long maxSeq, long creationTimeMillis) {
        return new PojoDataFileMeta(
                fileName,
                1024L,
                100L,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                minSeq,
                maxSeq,
                0L,
                0,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(creationTimeMillis),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
