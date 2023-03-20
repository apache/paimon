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

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newFile;
import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newSourceSplit;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link PendingSplitsCheckpointSerializer}. */
public class PendingSplitsCheckpointSerializerTest {

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
