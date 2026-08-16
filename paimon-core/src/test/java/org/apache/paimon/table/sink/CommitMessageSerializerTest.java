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

import java.io.IOException;
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
    }
}
