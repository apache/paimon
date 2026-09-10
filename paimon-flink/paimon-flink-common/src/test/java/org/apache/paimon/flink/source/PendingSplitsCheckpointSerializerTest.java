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

package org.apache.paimon.flink.source;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.utils.IOUtils;

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newFile;
import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newSourceSplit;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link PendingSplitsCheckpointSerializer}. */
public class PendingSplitsCheckpointSerializerTest {

    private static final String LEGACY_CHECKPOINT_RESOURCE =
            "compatibility/pending-splits-incremental-v1-chain-v2";

    @Test
    public void serializeEmptyCheckpoint() throws Exception {
        final PendingSplitsCheckpoint checkpoint =
                new PendingSplitsCheckpoint(Collections.emptyList(), 5L);

        final PendingSplitsCheckpoint deSerialized = serializeAndDeserialize(checkpoint);

        assertCheckpointsEqual(checkpoint, deSerialized);
    }

    @Test
    public void serializeSomeSplits() throws Exception {
        final PendingSplitsCheckpoint checkpoint =
                new PendingSplitsCheckpoint(
                        Arrays.asList(testSplit1(), testSplit2(), testSplit3()), 3L);

        final PendingSplitsCheckpoint deSerialized = serializeAndDeserialize(checkpoint);

        assertCheckpointsEqual(checkpoint, deSerialized);
    }

    @Test
    public void serializeSplitsAndContinuous() throws Exception {
        final PendingSplitsCheckpoint checkpoint =
                new PendingSplitsCheckpoint(
                        Arrays.asList(testSplit1(), testSplit2(), testSplit3()), 20L);

        final PendingSplitsCheckpoint deSerialized = serializeAndDeserialize(checkpoint);

        assertCheckpointsEqual(checkpoint, deSerialized);
    }

    @Test
    public void repeatedSerialization() throws Exception {
        final PendingSplitsCheckpoint checkpoint =
                new PendingSplitsCheckpoint(Arrays.asList(testSplit3(), testSplit1()), 5L);

        serializeAndDeserialize(checkpoint);
        serializeAndDeserialize(checkpoint);
        final PendingSplitsCheckpoint deSerialized = serializeAndDeserialize(checkpoint);

        assertCheckpointsEqual(checkpoint, deSerialized);
    }

    @Test
    public void restoreIncrementalAndChainSplits() throws Exception {
        DataFileMeta before = file("before.parquet", 1L);
        DataFileMeta after = file("after.parquet", 2L);
        IncrementalSplit incremental =
                new IncrementalSplit(
                        10L,
                        row(1),
                        2,
                        8,
                        Collections.singletonList(before),
                        Collections.singletonList(new DeletionFile("before.dv", 0L, 1L, 1L)),
                        Collections.singletonList(after),
                        Collections.singletonList(new DeletionFile("after.dv", 1L, 1L, 1L)),
                        true);

        Map<String, String> branchMapping = new LinkedHashMap<>();
        branchMapping.put(before.fileName(), "snapshot");
        branchMapping.put(after.fileName(), "delta");
        Map<String, String> bucketPathMapping = new LinkedHashMap<>();
        bucketPathMapping.put(before.fileName(), "dt=1/bucket-2");
        bucketPathMapping.put(after.fileName(), "dt=1/bucket-2");
        ChainSplit chain =
                new ChainSplit(
                        row(1),
                        Arrays.asList(before, after),
                        branchMapping,
                        bucketPathMapping,
                        Arrays.asList(null, new DeletionFile("chain.dv", 2L, 1L, 1L)));

        PendingSplitsCheckpoint restored =
                serializeAndDeserialize(
                        new PendingSplitsCheckpoint(
                                Arrays.asList(
                                        new FileStoreSourceSplit("incremental", incremental, 11L),
                                        new FileStoreSourceSplit("chain", chain, 12L)),
                                20L));

        assertThat(restored.currentSnapshotId()).isEqualTo(20L);
        assertThat(restored.splits()).hasSize(2);
        List<FileStoreSourceSplit> restoredSplits = new ArrayList<>(restored.splits());
        assertIncrementalSplit(restoredSplits.get(0), 11L);
        assertChainSplit(restoredSplits.get(1), 12L, branchMapping, bucketPathMapping);
    }

    @Test
    public void restoreLegacyIncrementalAndChainSplits() throws Exception {
        byte[] bytes;
        try (InputStream in =
                PendingSplitsCheckpointSerializerTest.class
                        .getClassLoader()
                        .getResourceAsStream(LEGACY_CHECKPOINT_RESOURCE)) {
            bytes = IOUtils.readFully(in, false);
        }
        PendingSplitsCheckpointSerializer serializer =
                new PendingSplitsCheckpointSerializer(new FileStoreSourceSplitSerializer());
        PendingSplitsCheckpoint restored =
                SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

        assertThat(restored.currentSnapshotId()).isEqualTo(20L);
        assertThat(restored.splits()).hasSize(2);
        List<FileStoreSourceSplit> restoredSplits = new ArrayList<>(restored.splits());

        FileStoreSourceSplit incrementalSource = restoredSplits.get(0);
        assertThat(incrementalSource.recordsToSkip()).isEqualTo(11L);
        assertThat(incrementalSource.split()).isInstanceOf(IncrementalSplit.class);
        IncrementalSplit incremental = (IncrementalSplit) incrementalSource.split();
        assertThat(incremental.beforeFiles())
                .extracting(DataFileMeta::fileName)
                .containsExactly("before.parquet");
        assertThat(incremental.afterFiles())
                .extracting(DataFileMeta::fileName)
                .containsExactly("after.parquet");
        assertThat(incremental.beforeFiles().get(0).columnMaxSequenceNumbers()).isNull();
        assertThat(incremental.afterFiles().get(0).columnMaxSequenceNumbers()).isNull();
        assertThat(incremental.beforeDeletionFiles())
                .containsExactly(new DeletionFile("before.dv", 0L, 1L, 1L));
        assertThat(incremental.afterDeletionFiles())
                .containsExactly(new DeletionFile("after.dv", 1L, 1L, 1L));

        FileStoreSourceSplit chainSource = restoredSplits.get(1);
        assertThat(chainSource.recordsToSkip()).isEqualTo(12L);
        assertThat(chainSource.split()).isInstanceOf(ChainSplit.class);
        ChainSplit chain = (ChainSplit) chainSource.split();
        assertThat(chain.dataFiles())
                .extracting(DataFileMeta::fileName)
                .containsExactly("before.parquet", "after.parquet");
        assertThat(chain.dataFiles())
                .allSatisfy(file -> assertThat(file.columnMaxSequenceNumbers()).isNull());
        assertThat(chain.fileBranchMapping())
                .containsOnly(
                        org.assertj.core.data.MapEntry.entry("before.parquet", "snapshot"),
                        org.assertj.core.data.MapEntry.entry("after.parquet", "delta"));
        assertThat(chain.fileBucketPathMapping())
                .containsOnly(
                        org.assertj.core.data.MapEntry.entry("before.parquet", "dt=1/bucket-2"),
                        org.assertj.core.data.MapEntry.entry("after.parquet", "dt=1/bucket-2"));
        assertThat(chain.deletionFiles())
                .hasValue(Arrays.asList(null, new DeletionFile("chain.dv", 2L, 1L, 1L)));
    }

    // ------------------------------------------------------------------------
    //  test utils
    // ------------------------------------------------------------------------

    private static FileStoreSourceSplit testSplit1() {
        return newSourceSplit("id1", row(1), 2, Arrays.asList(newFile(0), newFile(1)));
    }

    private static FileStoreSourceSplit testSplit2() {
        return newSourceSplit("id2", row(2), 3, Arrays.asList(newFile(2), newFile(3)));
    }

    private static FileStoreSourceSplit testSplit3() {
        return newSourceSplit("id3", row(3), 4, Arrays.asList(newFile(5), newFile(6)));
    }

    private static DataFileMeta file(String fileName, long sequence) {
        return newFile(0).rename(fileName).withColumnMaxSequenceNumbers(new long[] {sequence});
    }

    private static void assertIncrementalSplit(
            FileStoreSourceSplit sourceSplit, long recordsToSkip) {
        assertThat(sourceSplit.recordsToSkip()).isEqualTo(recordsToSkip);
        assertThat(sourceSplit.split()).isInstanceOf(IncrementalSplit.class);
        IncrementalSplit split = (IncrementalSplit) sourceSplit.split();
        assertColumnSequences(split.beforeFiles(), 1L);
        assertColumnSequences(split.afterFiles(), 2L);
        assertThat(split.beforeDeletionFiles())
                .containsExactly(new DeletionFile("before.dv", 0L, 1L, 1L));
        assertThat(split.afterDeletionFiles())
                .containsExactly(new DeletionFile("after.dv", 1L, 1L, 1L));
    }

    private static void assertChainSplit(
            FileStoreSourceSplit sourceSplit,
            long recordsToSkip,
            Map<String, String> branchMapping,
            Map<String, String> bucketPathMapping) {
        assertThat(sourceSplit.recordsToSkip()).isEqualTo(recordsToSkip);
        assertThat(sourceSplit.split()).isInstanceOf(ChainSplit.class);
        ChainSplit split = (ChainSplit) sourceSplit.split();
        assertColumnSequences(split.dataFiles(), 1L, 2L);
        assertThat(split.fileBranchMapping()).isEqualTo(branchMapping);
        assertThat(split.fileBucketPathMapping()).isEqualTo(bucketPathMapping);
        assertThat(split.deletionFiles())
                .hasValue(Arrays.asList(null, new DeletionFile("chain.dv", 2L, 1L, 1L)));
    }

    private static void assertColumnSequences(List<DataFileMeta> files, long... sequences) {
        assertThat(files).hasSize(sequences.length);
        for (int i = 0; i < sequences.length; i++) {
            assertThat(files.get(i).columnMaxSequenceNumbers()).containsExactly(sequences[i]);
        }
    }

    private static PendingSplitsCheckpoint serializeAndDeserialize(
            final PendingSplitsCheckpoint split) throws IOException {

        final PendingSplitsCheckpointSerializer serializer =
                new PendingSplitsCheckpointSerializer(new FileStoreSourceSplitSerializer());
        final byte[] bytes =
                SimpleVersionedSerialization.writeVersionAndSerialize(serializer, split);
        return SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);
    }

    private static void assertCheckpointsEqual(
            final PendingSplitsCheckpoint expected, final PendingSplitsCheckpoint actual) {
        assertThat(actual.splits()).isEqualTo(expected.splits());
        assertThat(actual.currentSnapshotId()).isEqualTo(expected.currentSnapshotId());
    }
}
