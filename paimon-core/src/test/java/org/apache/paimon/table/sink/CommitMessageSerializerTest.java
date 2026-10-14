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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

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

        CommitMessageImpl checked = committable.withCheckFromSnapshot(42L);
        CommitMessageImpl checkedRoundTrip =
                (CommitMessageImpl)
                        serializer.deserialize(
                                serializer.getVersion(), serializer.serialize(checked));
        assertThat(checkedRoundTrip).isEqualTo(checked);
        assertThat(checkedRoundTrip.checkFromSnapshot()).isEqualTo(42L);

        byte[] serializedWithoutSnapshot = serializer.serialize(committable);
        CommitMessageImpl oldVersion =
                (CommitMessageImpl)
                        serializer.deserialize(
                                13,
                                Arrays.copyOf(
                                        serializedWithoutSnapshot,
                                        serializedWithoutSnapshot.length - 2));
        assertThat(oldVersion.checkFromSnapshot()).isNull();
        assertThat(oldVersion.compactIncrement().hasDeletionVectorBaseline()).isFalse();
        assertThat(oldVersion.compactIncrement().expectedDeletionVectorFile()).isNull();
        assertThat(oldVersion.newFilesIncrement()).isEqualTo(committable.newFilesIncrement());

        CommitMessageImpl version14 =
                (CommitMessageImpl)
                        serializer.deserialize(
                                14,
                                Arrays.copyOf(
                                        serializedWithoutSnapshot,
                                        serializedWithoutSnapshot.length - 1));
        assertThat(version14).isEqualTo(committable);
        assertThat(version14.compactIncrement().hasDeletionVectorBaseline()).isFalse();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "dv-1")
    public void testDeletionVectorBaselineRoundTrip(@Nullable String expected) throws Exception {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        CompactIncrement increment = randomCompactIncrement();
        increment.setExpectedDeletionVectorFile(expected);
        CommitMessageImpl message =
                new CommitMessageImpl(row(0), 1, 2, randomNewFilesIncrement(), increment);
        CommitMessageImpl restored =
                (CommitMessageImpl)
                        serializer.deserialize(
                                serializer.getVersion(), serializer.serialize(message));
        assertThat(restored).isEqualTo(message);
        assertThat(restored.compactIncrement().hasDeletionVectorBaseline()).isTrue();
        assertThat(restored.compactIncrement().expectedDeletionVectorFile()).isEqualTo(expected);
    }

    @Test
    public void testUnknownAndEmptyDeletionVectorBaselinesAreDifferent() {
        CompactIncrement unknown = CompactIncrement.emptyIncrement();
        CompactIncrement empty = CompactIncrement.emptyIncrement();
        empty.setExpectedDeletionVectorFile(null);
        assertThat(empty).isNotEqualTo(unknown);
        assertThat(empty.hasDeletionVectorBaseline()).isTrue();
        assertThat(unknown.hasDeletionVectorBaseline()).isFalse();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "dv-1")
    public void testJavaSerializationPreservesCheckFromSnapshot(@Nullable String expected)
            throws Exception {
        CommitMessageImpl message =
                new CommitMessageImpl(
                                row(0),
                                1,
                                null,
                                randomNewFilesIncrement(),
                                CompactIncrement.emptyIncrement())
                        .withCheckFromSnapshot(42L);
        message.compactIncrement().setExpectedDeletionVectorFile(expected);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(message);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            CommitMessageImpl restored = (CommitMessageImpl) input.readObject();
            assertThat(restored).isEqualTo(message);
            assertThat(restored.checkFromSnapshot()).isEqualTo(42L);
        }
    }
}
