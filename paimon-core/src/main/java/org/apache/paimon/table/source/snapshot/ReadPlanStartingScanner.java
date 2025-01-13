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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** An {@link AbstractStartingScanner} to return plan. */
public abstract class ReadPlanStartingScanner extends AbstractStartingScanner {

    ReadPlanStartingScanner(SnapshotManager snapshotManager) {
        super(snapshotManager);
    }

    @Nullable
    protected abstract SnapshotReader configure(SnapshotReader snapshotReader);

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        SnapshotReader configured = configure(snapshotReader);
        if (configured == null) {
            return new NoSnapshot();
        }
        return StartingScanner.fromPlan(configured.read());
    }

    @Override
    public List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader) {
        SnapshotReader configured = configure(snapshotReader);
        if (configured == null) {
            return Collections.emptyList();
        }
        return configured.partitionEntries();
    }
}
