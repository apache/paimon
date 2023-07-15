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
import java.util.List;
import java.util.Map;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.manifest.ManifestFileMetaTestBase.makeEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CommitStats}. */
public class CommitStatTest {
    private static List<ManifestEntry> files;

    @BeforeAll
    public static void beforeAll() {
        files = new ArrayList<>();
        files.add(makeEntry(FileKind.ADD, 1, 1, 201));
        files.add(makeEntry(FileKind.ADD, 2, 3, 302));
        files.add(makeEntry(FileKind.ADD, 1, 1, 202));
        files.add(makeEntry(FileKind.ADD, 2, 3, 301));
        files.add(makeEntry(FileKind.ADD, 1, 1, 203));
        files.add(makeEntry(FileKind.ADD, 2, 3, 304));
        files.add(makeEntry(FileKind.DELETE, 3, 5, 106));
        files.add(makeEntry(FileKind.ADD, 1, 1, 205));
        files.add(makeEntry(FileKind.ADD, 2, 3, 307));
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
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> bucketedFiles = CommitStats.groupByBucket(files);
        assertEquals(4, bucketedFiles.get(1).get(1).size());
        assertEquals(0, bucketedFiles.get(1).get(3).size());
        assertEquals(0, bucketedFiles.get(1).get(5).size());
        assertEquals(0, bucketedFiles.get(2).get(1).size());
        assertEquals(0, bucketedFiles.get(2).get(3).size());
        assertEquals(0, bucketedFiles.get(2).get(5).size());
        assertEquals(0, bucketedFiles.get(3).get(1).size());
        assertEquals(0, bucketedFiles.get(3).get(3).size());
        assertEquals(1, bucketedFiles.get(3).get(5).size());
    }

    @Test
    public void testCalcChangedPartitionsAndBuckets() {
        assertEquals(3, CommitStats.numChangedBuckets(files));
        assertEquals(3, CommitStats.numChangedPartitions(files));
        assertTrue(CommitStats.changedPartBuckets(files).get(1).containsAll(Arrays.asList(1)));
        assertTrue(CommitStats.changedPartBuckets(files).get(2).containsAll(Arrays.asList(3)));
        assertTrue(CommitStats.changedPartBuckets(files).get(3).containsAll(Arrays.asList(5)));
        assertTrue(
                CommitStats.changedPartitions(files)
                        .containsAll(Arrays.asList(row(1), row(2), row(3))));
    }
}
