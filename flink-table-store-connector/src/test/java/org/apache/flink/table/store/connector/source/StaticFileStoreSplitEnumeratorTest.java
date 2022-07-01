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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.flink.table.store.connector.source.FileStoreSourceSplitSerializerTest.newFile;
import static org.apache.flink.table.store.connector.source.FileStoreSourceSplitSerializerTest.newSourceSplit;
import static org.apache.flink.table.store.file.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link StaticFileStoreSplitEnumerator}. */
public class StaticFileStoreSplitEnumeratorTest {

    @Test
    public void testCheckpointNoSplitRequested() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        final FileStoreSourceSplit split = createRandomSplit();
        final StaticFileStoreSplitEnumerator enumerator = createEnumerator(context, split);

        final PendingSplitsCheckpoint checkpoint = enumerator.snapshotState(1L);

        assertThat(checkpoint.splits()).contains(split);
    }

    @Test
    public void testSplitRequestForRegisteredReader() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        final FileStoreSourceSplit split = createRandomSplit();
        final StaticFileStoreSplitEnumerator enumerator = createEnumerator(context, split);

        context.registerReader(3, "somehost");
        enumerator.addReader(3);
        enumerator.handleSplitRequest(3, "somehost");

        assertThat(enumerator.snapshotState(1L).splits()).isEmpty();
        assertThat(context.getSplitAssignments().get(3).getAssignedSplits()).contains(split);
    }

    @Test
    public void testSplitRequestForNonRegisteredReader() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        final FileStoreSourceSplit split = createRandomSplit();
        final StaticFileStoreSplitEnumerator enumerator = createEnumerator(context, split);

        enumerator.handleSplitRequest(3, "somehost");

        assertThat(context.getSplitAssignments().containsKey(3)).isFalse();
        assertThat(enumerator.snapshotState(1L).splits()).contains(split);
    }

    @Test
    public void testNoMoreSplits() {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        final FileStoreSourceSplit split = createRandomSplit();
        final StaticFileStoreSplitEnumerator enumerator = createEnumerator(context, split);

        // first split assignment
        context.registerReader(1, "somehost");
        enumerator.addReader(1);
        enumerator.handleSplitRequest(1, "somehost");

        // second request has no more split
        enumerator.handleSplitRequest(1, "somehost");

        assertThat(context.getSplitAssignments().get(1).getAssignedSplits()).contains(split);
        assertThat(context.getSplitAssignments().get(1).hasReceivedNoMoreSplitsSignal()).isTrue();
    }

    // ------------------------------------------------------------------------
    //  test setup helpers
    // ------------------------------------------------------------------------

    private static FileStoreSourceSplit createRandomSplit() {
        return newSourceSplit("split", row(1), 2, Arrays.asList(newFile(0), newFile(1)));
    }

    private static StaticFileStoreSplitEnumerator createEnumerator(
            final SplitEnumeratorContext<FileStoreSourceSplit> context,
            final FileStoreSourceSplit... splits) {
        return new StaticFileStoreSplitEnumerator(context, null, Arrays.asList(splits));
    }
}
