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

package org.apache.paimon.operation.commit;

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageList;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomCompactIncrement;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests replaying all kinds of file changes without retaining decoded messages. */
class ManifestEntryChangesTest {

    @Test
    void testReplayMatchesEagerOrderAndBucketFallback() throws IOException {
        List<CommitMessage> originals = Arrays.asList(message(null), message(8));
        List<CommitMessage> messages =
                CommitMessageList.fromBytes(CommitMessageSerializer.serializeAll(originals));
        ManifestEntryChanges expected = new ManifestEntryChanges(4);
        originals.forEach(expected::collect);
        ManifestEntryChanges actual = ManifestEntryChanges.lazyChanges(4, messages);
        for (int pass = 0; pass < 2; pass++) {
            assertThat(actual.appendTableFiles)
                    .containsExactlyElementsOf(expected.appendTableFiles);
            assertThat(actual.appendChangelog).containsExactlyElementsOf(expected.appendChangelog);
            assertThat(actual.appendIndexFiles)
                    .containsExactlyElementsOf(expected.appendIndexFiles);
            assertThat(actual.compactTableFiles)
                    .containsExactlyElementsOf(expected.compactTableFiles);
            assertThat(actual.compactChangelog)
                    .containsExactlyElementsOf(expected.compactChangelog);
            assertThat(actual.compactIndexFiles)
                    .containsExactlyElementsOf(expected.compactIndexFiles);
        }
        assertThat(actual.appendTableFiles.get(0)).isNotSameAs(actual.appendTableFiles.get(0));
        assertThat(actual.appendTableFiles.subList(1, 3))
                .containsExactlyElementsOf(expected.appendTableFiles.subList(1, 3));
    }

    private CommitMessage message(Integer totalBuckets) {
        return new CommitMessageImpl(
                row(1), 2, totalBuckets, randomNewFilesIncrement(), randomCompactIncrement());
    }
}
