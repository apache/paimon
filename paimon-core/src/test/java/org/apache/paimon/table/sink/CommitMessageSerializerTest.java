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

package org.apache.paimon.table.sink;

import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import static org.apache.paimon.index.IndexFileMetaSerializerTest.randomIndexFile;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomCompactIncrement;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CommitMessageSerializer}. */
public class CommitMessageSerializerTest {

    @Test
    public void test() throws IOException {
        CommitMessageSerializer serializer = new CommitMessageSerializer();

        DataIncrement dataIncrement = randomNewFilesIncrement();
        dataIncrement
                .newFiles()
                .set(
                        0,
                        dataIncrement
                                .newFiles()
                                .get(0)
                                .withWriteColsSequences(new long[] {3L, 42L}));
        dataIncrement.newIndexFiles().addAll(Arrays.asList(randomIndexFile(), randomIndexFile()));
        dataIncrement
                .deletedIndexFiles()
                .addAll(Arrays.asList(randomIndexFile(), randomIndexFile()));

        CompactIncrement compactIncrement = randomCompactIncrement();
        compactIncrement
                .newIndexFiles()
                .addAll(Arrays.asList(randomIndexFile(), randomIndexFile()));
        compactIncrement
                .deletedIndexFiles()
                .addAll(Arrays.asList(randomIndexFile(), randomIndexFile()));

        CommitMessageImpl committable =
                new CommitMessageImpl(row(0), 1, 2, dataIncrement, compactIncrement);

        CommitMessageImpl newCommittable =
                (CommitMessageImpl)
                        serializer.deserialize(
                                CommitMessageSerializer.CURRENT_VERSION,
                                serializer.serialize(committable));
        assertThat(newCommittable.partition()).isEqualTo(committable.partition());
        assertThat(newCommittable.bucket()).isEqualTo(committable.bucket());
        assertThat(newCommittable.totalBuckets()).isEqualTo(committable.totalBuckets());
        assertThat(newCommittable.compactIncrement()).isEqualTo(committable.compactIncrement());
        assertThat(newCommittable.newFilesIncrement()).isEqualTo(committable.newFilesIncrement());
        assertThat(newCommittable.checkFromSnapshot()).isNull();
        assertThat(newCommittable.deleteOrderingGuardGeneration()).isNull();
        assertThat(newCommittable.deleteOrderingGuardSources()).isNull();

        CommitMessageImpl checked =
                committable
                        .withDeleteOrderingGuardSources(Arrays.asList("guard-1", "guard-2"))
                        .withDeleteOrderingGuardGeneration("generation-A")
                        .withCheckFromSnapshot(42L);
        CommitMessageImpl checkedRoundTrip =
                (CommitMessageImpl)
                        serializer.deserialize(
                                serializer.getVersion(), serializer.serialize(checked));
        assertThat(checkedRoundTrip).isEqualTo(checked);
        assertThat(checkedRoundTrip.checkFromSnapshot()).isEqualTo(42L);
        assertThat(checkedRoundTrip.deleteOrderingGuardGeneration()).isEqualTo("generation-A");
        assertThat(checkedRoundTrip.deleteOrderingGuardSources())
                .containsExactly("guard-1", "guard-2");

        byte[] serializedWithoutSnapshot = serializer.serialize(committable);
        CommitMessageImpl oldVersion =
                (CommitMessageImpl)
                        serializer.deserialize(
                                13,
                                Arrays.copyOf(
                                        serializedWithoutSnapshot,
                                        serializedWithoutSnapshot.length - 3));
        assertThat(oldVersion.checkFromSnapshot()).isNull();
        assertThat(oldVersion.newFilesIncrement()).isEqualTo(committable.newFilesIncrement());

        CommitMessageImpl version14 =
                (CommitMessageImpl)
                        serializer.deserialize(
                                14,
                                Arrays.copyOf(
                                        serializedWithoutSnapshot,
                                        serializedWithoutSnapshot.length - 2));
        assertThat(version14.deleteOrderingGuardGeneration()).isNull();
        assertThat(version14).isEqualTo(committable);

        CommitMessageImpl version15 =
                (CommitMessageImpl)
                        serializer.deserialize(
                                15,
                                Arrays.copyOf(
                                        serializedWithoutSnapshot,
                                        serializedWithoutSnapshot.length - 1));
        assertThat(version15).isEqualTo(committable);
        assertThat(version15.deleteOrderingGuardSources()).isNull();

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, new byte[0]))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Upgrade the Paimon reader");
    }

    @Test
    public void testJavaSerializationPreservesCheckFromSnapshot() throws Exception {
        CommitMessageImpl message =
                new CommitMessageImpl(
                                row(0),
                                1,
                                null,
                                randomNewFilesIncrement(),
                                CompactIncrement.emptyIncrement())
                        .withCheckFromSnapshot(42L)
                        .withDeleteOrderingGuardSources(Arrays.asList("guard-3", "guard-4"))
                        .withDeleteOrderingGuardGeneration("generation-B");

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(message);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            CommitMessageImpl restored = (CommitMessageImpl) input.readObject();
            assertThat(restored).isEqualTo(message);
            assertThat(restored.checkFromSnapshot()).isEqualTo(42L);
            assertThat(restored.deleteOrderingGuardGeneration()).isEqualTo("generation-B");
            assertThat(restored.deleteOrderingGuardSources()).containsExactly("guard-3", "guard-4");
        }
    }
}
