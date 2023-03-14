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

package org.apache.flink.table.store.table.source.snapshot;

import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.sink.TableCommitImpl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BoundedWatermarkFollowUpScanner}. */
public class BoundedWatermarkFollowUpScannerTest extends ScannerTestBase {

    @Test
    public void testBounded() {
        SnapshotManager snapshotManager = table.snapshotManager();
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);
        FollowUpScanner scanner =
                new BoundedWatermarkFollowUpScanner(new DeltaFollowUpScanner(), 2000L);

        commit.commit(new ManifestCommittable(0, 1024L));
        Snapshot snapshot = snapshotManager.latestSnapshot();
        assertThat(scanner.shouldEndInput(snapshot)).isFalse();
        assertThat(scanner.getPlan(snapshot.id(), snapshotSplitReader).splits()).isEmpty();

        commit.commit(new ManifestCommittable(0, 2000L));
        snapshot = snapshotManager.latestSnapshot();
        assertThat(scanner.shouldEndInput(snapshot)).isFalse();
        assertThat(scanner.getPlan(snapshot.id(), snapshotSplitReader).splits()).isEmpty();

        commit.commit(new ManifestCommittable(0, 2001L));
        snapshot = snapshotManager.latestSnapshot();
        assertThat(scanner.shouldEndInput(snapshot)).isTrue();
    }
}
