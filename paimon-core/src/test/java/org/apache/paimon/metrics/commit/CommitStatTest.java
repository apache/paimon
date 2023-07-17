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

package org.apache.paimon.metrics.commit;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.manifest.ManifestFileMetaTestBase.makeEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CommitStats}. */
public class CommitStatTest {
    private static List<ManifestEntry> files = new ArrayList<>();
    private static List<ManifestEntry> appendDataFiles = new ArrayList<>();
    private static List<ManifestEntry> appendChangelogFiles = new ArrayList<>();
    private static List<ManifestEntry> compactDataFiles = new ArrayList<>();
    private static List<ManifestEntry> compactChangelogFiles = new ArrayList<>();

    @BeforeAll
    public static void beforeAll() {
        appendDataFiles.add(makeEntry(FileKind.ADD, 1, 1, 201));
        appendDataFiles.add(makeEntry(FileKind.ADD, 2, 3, 302));
        appendChangelogFiles.add(makeEntry(FileKind.ADD, 1, 1, 202));
        appendChangelogFiles.add(makeEntry(FileKind.ADD, 2, 3, 301));
        compactDataFiles.add(makeEntry(FileKind.ADD, 1, 1, 203));
        compactDataFiles.add(makeEntry(FileKind.ADD, 2, 3, 304));
        compactDataFiles.add(makeEntry(FileKind.DELETE, 3, 5, 106));
        compactChangelogFiles.add(makeEntry(FileKind.ADD, 1, 1, 205));
        compactChangelogFiles.add(makeEntry(FileKind.ADD, 2, 3, 307));
        files.addAll(appendDataFiles);
        files.addAll(appendChangelogFiles);
        files.addAll(compactDataFiles);
        files.addAll(compactChangelogFiles);
    }

    @Test
    public void testGroupByPartition() {
        Map<BinaryRow, List<DataFileMeta>> partitionedFiles = CommitStats.groupByPartititon(files);
        assertEquals(4, partitionedFiles.get(row(1)).size());
        assertEquals(4, partitionedFiles.get(row(2)).size());
        assertEquals(1, partitionedFiles.get(row(3)).size());
    }

    @Test
    public void testGroupByBucket() {
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> bucketedFiles =
                CommitStats.groupByBucket(files);
        assertEquals(
                4,
                bucketedFiles
                        .getOrDefault(row(1), new HashMap<>())
                        .getOrDefault(1, new ArrayList<>())
                        .size());
        assertEquals(
                0,
                bucketedFiles
                        .getOrDefault(row(1), new HashMap<>())
                        .getOrDefault(3, new ArrayList<>())
                        .size());
        assertEquals(
                0,
                bucketedFiles
                        .getOrDefault(row(1), new HashMap<>())
                        .getOrDefault(5, new ArrayList<>())
                        .size());
        assertEquals(
                0,
                bucketedFiles
                        .getOrDefault(row(2), new HashMap<>())
                        .getOrDefault(1, new ArrayList<>())
                        .size());
        assertEquals(
                4,
                bucketedFiles
                        .getOrDefault(row(2), new HashMap<>())
                        .getOrDefault(3, new ArrayList<>())
                        .size());
        assertEquals(
                0,
                bucketedFiles
                        .getOrDefault(row(2), new HashMap<>())
                        .getOrDefault(5, new ArrayList<>())
                        .size());
        assertEquals(
                0,
                bucketedFiles
                        .getOrDefault(row(3), new HashMap<>())
                        .getOrDefault(1, new ArrayList<>())
                        .size());
        assertEquals(
                0,
                bucketedFiles
                        .getOrDefault(row(3), new HashMap<>())
                        .getOrDefault(3, new ArrayList<>())
                        .size());
        assertEquals(
                1,
                bucketedFiles
                        .getOrDefault(row(3), new HashMap<>())
                        .getOrDefault(5, new ArrayList<>())
                        .size());
    }

    @Test
    public void testCalcChangedPartitionsAndBuckets() {
        assertEquals(3, CommitStats.numChangedBuckets(files));
        assertEquals(3, CommitStats.numChangedPartitions(files));
        assertTrue(CommitStats.changedPartBuckets(files).get(row(1)).containsAll(Arrays.asList(1)));
        assertTrue(CommitStats.changedPartBuckets(files).get(row(2)).containsAll(Arrays.asList(3)));
        assertTrue(CommitStats.changedPartBuckets(files).get(row(3)).containsAll(Arrays.asList(5)));
        assertTrue(
                CommitStats.changedPartitions(files)
                        .containsAll(Arrays.asList(row(1), row(2), row(3))));
    }

    @Test
    public void testFailedAppendSnapshot() {
        CommitStats commitStats =
                new CommitStats(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        0,
                        0,
                        1);
        assertEquals(0, commitStats.getTableFilesAdded());
        assertEquals(0, commitStats.getBucketedTableFilesAdded(row(1), 2));
        assertEquals(0, commitStats.getTableFilesDeleted());
        assertEquals(0, commitStats.getBucketedTableFilesDeleted(row(1), 2));
        assertEquals(0, commitStats.getBucketedTableFilesAppended(row(1), 2));
        assertEquals(0, commitStats.getBucketedTableFilesCompacted(row(1), 2));
        assertEquals(0, commitStats.getChangelogFilesCommitAppended());
        assertEquals(0, commitStats.getBucketedChangelogFilesAppended(row(1), 2));
        assertEquals(0, commitStats.getChangelogFilesCompacted());
        assertEquals(0, commitStats.getBucketedChangelogFilesCompacted(row(1), 2));
        assertEquals(0, commitStats.getGeneratedSnapshots());
        assertEquals(0, commitStats.getBucketedDeltaRecordsAppended(row(1), 2));
        assertEquals(0, commitStats.getBucketedChangelogRecordsAppended(row(1), 2));
        assertEquals(0, commitStats.getBucketedDeltaRecordsCompacted(row(1), 2));
        assertEquals(0, commitStats.getBucketedChangelogRecordsCompacted(row(1), 2));
        assertEquals(0, commitStats.getNumPartitionsWritten());
        assertEquals(0, commitStats.getNumBucketsWritten());
        assertEquals(0, commitStats.getDuration());
        assertEquals(1, commitStats.getAttempts());
    }

    @Test
    public void testFailedCompactSnapshot() {
        CommitStats commitStats =
                new CommitStats(
                        appendDataFiles,
                        appendChangelogFiles,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        3000,
                        1,
                        2);
        assertEquals(2, commitStats.getTableFilesAdded());
        assertEquals(1, commitStats.getBucketedTableFilesAdded(row(1), 1));
        assertEquals(0, commitStats.getTableFilesDeleted());
        assertEquals(0, commitStats.getBucketedTableFilesDeleted(row(1), 1));
        assertEquals(1, commitStats.getBucketedTableFilesAppended(row(2), 3));
        assertEquals(0, commitStats.getBucketedTableFilesCompacted(row(1), 1));
        assertEquals(2, commitStats.getChangelogFilesCommitAppended());
        assertEquals(1, commitStats.getBucketedChangelogFilesAppended(row(2), 3));
        assertEquals(0, commitStats.getChangelogFilesCompacted());
        assertEquals(0, commitStats.getBucketedChangelogFilesCompacted(row(1), 1));
        assertEquals(1, commitStats.getGeneratedSnapshots());
        assertEquals(201, commitStats.getBucketedDeltaRecordsAppended(row(1), 1));
        assertEquals(202, commitStats.getBucketedChangelogRecordsAppended(row(1), 1));
        assertEquals(0, commitStats.getBucketedDeltaRecordsCompacted(row(2), 3));
        assertEquals(0, commitStats.getBucketedChangelogRecordsCompacted(row(2), 3));
        assertEquals(2, commitStats.getNumPartitionsWritten());
        assertEquals(2, commitStats.getNumBucketsWritten());
        assertEquals(3000, commitStats.getDuration());
        assertEquals(2, commitStats.getAttempts());
    }

    @Test
    public void testSucceedAllSnapshot() {
        CommitStats commitStats =
                new CommitStats(
                        appendDataFiles,
                        appendChangelogFiles,
                        compactDataFiles,
                        compactChangelogFiles,
                        3000,
                        2,
                        2);
        assertEquals(4, commitStats.getTableFilesAdded());
        assertEquals(2, commitStats.getBucketedTableFilesAdded(row(1), 1));
        assertEquals(1, commitStats.getTableFilesDeleted());
        assertEquals(0, commitStats.getBucketedTableFilesDeleted(row(1), 1));
        assertEquals(1, commitStats.getBucketedTableFilesAppended(row(2), 3));
        assertEquals(1, commitStats.getBucketedTableFilesCompacted(row(1), 1));
        assertEquals(2, commitStats.getChangelogFilesCommitAppended());
        assertEquals(1, commitStats.getBucketedChangelogFilesAppended(row(2), 3));
        assertEquals(2, commitStats.getChangelogFilesCompacted());
        assertEquals(1, commitStats.getBucketedChangelogFilesCompacted(row(1), 1));
        assertEquals(2, commitStats.getGeneratedSnapshots());
        assertEquals(201, commitStats.getBucketedDeltaRecordsAppended(row(1), 1));
        assertEquals(202, commitStats.getBucketedChangelogRecordsAppended(row(1), 1));
        assertEquals(304, commitStats.getBucketedDeltaRecordsCompacted(row(2), 3));
        assertEquals(307, commitStats.getBucketedChangelogRecordsCompacted(row(2), 3));
        assertEquals(3, commitStats.getNumPartitionsWritten());
        assertEquals(3, commitStats.getNumBucketsWritten());
        assertEquals(3000, commitStats.getDuration());
        assertEquals(2, commitStats.getAttempts());
    }
}
