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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.manifest.ManifestFileMetaTestBase.makeEntry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.paimon.operation.metrics.CommitStats}. */
public class CommitStatsTest {
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
    public void testCalcChangedPartitionsAndBuckets() {
        assertThat(CommitStats.numChangedBuckets(files)).isEqualTo(3);
        assertThat(CommitStats.numChangedPartitions(files)).isEqualTo(3);
        assertThat(CommitStats.changedPartBuckets(files).get(row(1))).containsExactly(1);
        assertThat(CommitStats.changedPartBuckets(files).get(row(2))).containsExactly(3);
        assertThat(CommitStats.changedPartBuckets(files).get(row(3))).containsExactly(5);
        assertThat(CommitStats.changedPartitions(files))
                .containsExactlyInAnyOrder(row(1), row(2), row(3));
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
        assertThat(commitStats.getTableFilesAdded()).isEqualTo(0);
        assertThat(commitStats.getTableFilesDeleted()).isEqualTo(0);
        assertThat(commitStats.getTableFilesAppended()).isEqualTo(0);
        assertThat(commitStats.getTableFilesCompacted()).isEqualTo(0);
        assertThat(commitStats.getChangelogFilesAppended()).isEqualTo(0);
        assertThat(commitStats.getChangelogFilesCompacted()).isEqualTo(0);
        assertThat(commitStats.getGeneratedSnapshots()).isEqualTo(0);
        assertThat(commitStats.getDeltaRecordsAppended()).isEqualTo(0);
        assertThat(commitStats.getChangelogRecordsAppended()).isEqualTo(0);
        assertThat(commitStats.getDeltaRecordsCompacted()).isEqualTo(0);
        assertThat(commitStats.getChangelogRecordsCompacted()).isEqualTo(0);
        assertThat(commitStats.getNumPartitionsWritten()).isEqualTo(0);
        assertThat(commitStats.getNumBucketsWritten()).isEqualTo(0);
        assertThat(commitStats.getDuration()).isEqualTo(0);
        assertThat(commitStats.getAttempts()).isEqualTo(1);
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
        assertThat(commitStats.getTableFilesAdded()).isEqualTo(2);
        assertThat(commitStats.getTableFilesDeleted()).isEqualTo(0);
        assertThat(commitStats.getTableFilesAppended()).isEqualTo(2);
        assertThat(commitStats.getTableFilesCompacted()).isEqualTo(0);
        assertThat(commitStats.getChangelogFilesAppended()).isEqualTo(2);
        assertThat(commitStats.getChangelogFilesCompacted()).isEqualTo(0);
        assertThat(commitStats.getGeneratedSnapshots()).isEqualTo(1);
        assertThat(commitStats.getDeltaRecordsAppended()).isEqualTo(503);
        assertThat(commitStats.getChangelogRecordsAppended()).isEqualTo(503);
        assertThat(commitStats.getDeltaRecordsCompacted()).isEqualTo(0);
        assertThat(commitStats.getChangelogRecordsCompacted()).isEqualTo(0);
        assertThat(commitStats.getNumPartitionsWritten()).isEqualTo(2);
        assertThat(commitStats.getNumBucketsWritten()).isEqualTo(2);
        assertThat(commitStats.getDuration()).isEqualTo(3000);
        assertThat(commitStats.getAttempts()).isEqualTo(2);
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
        assertThat(commitStats.getTableFilesAdded()).isEqualTo(4);
        assertThat(commitStats.getTableFilesDeleted()).isEqualTo(1);
        assertThat(commitStats.getTableFilesAppended()).isEqualTo(2);
        assertThat(commitStats.getTableFilesCompacted()).isEqualTo(3);
        assertThat(commitStats.getChangelogFilesAppended()).isEqualTo(2);
        assertThat(commitStats.getChangelogFilesCompacted()).isEqualTo(2);
        assertThat(commitStats.getGeneratedSnapshots()).isEqualTo(2);
        assertThat(commitStats.getDeltaRecordsAppended()).isEqualTo(503);
        assertThat(commitStats.getChangelogRecordsAppended()).isEqualTo(503);
        assertThat(commitStats.getDeltaRecordsCompacted()).isEqualTo(613);
        assertThat(commitStats.getChangelogRecordsCompacted()).isEqualTo(512);
        assertThat(commitStats.getNumPartitionsWritten()).isEqualTo(3);
        assertThat(commitStats.getNumBucketsWritten()).isEqualTo(3);
        assertThat(commitStats.getDuration()).isEqualTo(3000);
        assertThat(commitStats.getAttempts()).isEqualTo(2);
    }
}
