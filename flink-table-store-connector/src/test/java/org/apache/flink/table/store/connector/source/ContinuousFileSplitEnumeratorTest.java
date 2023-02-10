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
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.snapshot.SnapshotEnumerator;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.table.store.file.mergetree.compact.MergeTreeCompactManagerTest.row;

/** Unit tests for the {@link ContinuousFileSplitEnumerator}. */
public class ContinuousFileSplitEnumeratorTest {

    @Test
    public void testSplitAllocationIsOrdered() throws Exception {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(1);
        context.registerReader(0, "test-host");

        List<FileStoreSourceSplit> initialSplits = new ArrayList<>();
        FileStoreSourceSplit split1 = createSnapshotSplit(1, 0, Collections.emptyList());
        FileStoreSourceSplit split2 = createSnapshotSplit(2, 0, Collections.emptyList());
        initialSplits.add(split1);
        initialSplits.add(split2);
        final ContinuousFileSplitEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(initialSplits)
                        .setDiscoveryInterval(3)
                        .build();

        // The first time split is allocated, split1 should be allocated
        enumerator.handleSplitRequest(0, "test-host");
        Assert.assertEquals(1, context.getSplitAssignments().size());
        Assert.assertTrue(context.getSplitAssignments().containsKey(0));
        List<FileStoreSourceSplit> assignedSplits =
                context.getSplitAssignments().get(0).getAssignedSplits();
        Assert.assertEquals(1, assignedSplits.size());
        Assert.assertEquals(split1, assignedSplits.get(0));

        // split1 is added back
        enumerator.addSplitsBack(Collections.singletonList(split1), 0);
        context.getSplitAssignments().clear();
        Assert.assertEquals(0, context.getSplitAssignments().size());

        // The split is allocated for the second time, and split1 is allocated first
        enumerator.handleSplitRequest(0, "test-host");
        Assert.assertEquals(1, context.getSplitAssignments().size());
        Assert.assertTrue(context.getSplitAssignments().containsKey(0));
        assignedSplits = context.getSplitAssignments().get(0).getAssignedSplits();
        Assert.assertEquals(1, assignedSplits.size());
        Assert.assertEquals(split1, assignedSplits.get(0));

        // continuing to allocate split, split2 is allocated.
        context.getSplitAssignments().clear();
        enumerator.handleSplitRequest(0, "test-host");
        Assert.assertEquals(1, context.getSplitAssignments().size());
        Assert.assertTrue(context.getSplitAssignments().containsKey(0));
        assignedSplits = context.getSplitAssignments().get(0).getAssignedSplits();
        Assert.assertEquals(1, assignedSplits.size());
        Assert.assertEquals(split2, assignedSplits.get(0));
    }

    private static FileStoreSourceSplit createSnapshotSplit(
            int snapshotId, int bucket, List<DataFileMeta> files) {
        return new FileStoreSourceSplit(
                UUID.randomUUID().toString(),
                new DataSplit(snapshotId, row(1), bucket, files, true),
                0);
    }

    private static class Builder {
        private SplitEnumeratorContext<FileStoreSourceSplit> context;
        private Collection<FileStoreSourceSplit> initialSplits = Collections.emptyList();
        private Long nextSnapshotId;
        private long discoveryInterval = Long.MAX_VALUE;
        private SnapshotEnumerator snapshotEnumerator;

        public Builder setSplitEnumeratorContext(
                SplitEnumeratorContext<FileStoreSourceSplit> context) {
            this.context = context;
            return this;
        }

        public Builder setInitialSplits(Collection<FileStoreSourceSplit> initialSplits) {
            this.initialSplits = initialSplits;
            return this;
        }

        public Builder setNextSnapshotId(Long nextSnapshotId) {
            this.nextSnapshotId = nextSnapshotId;
            return this;
        }

        public Builder setDiscoveryInterval(long discoveryInterval) {
            this.discoveryInterval = discoveryInterval;
            return this;
        }

        public Builder setSnapshotEnumerator(SnapshotEnumerator snapshotEnumerator) {
            this.snapshotEnumerator = snapshotEnumerator;
            return this;
        }

        public ContinuousFileSplitEnumerator build() {
            return new ContinuousFileSplitEnumerator(
                    context, initialSplits, nextSnapshotId, discoveryInterval, snapshotEnumerator);
        }
    }
}
