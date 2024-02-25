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

package org.apache.paimon.flink.sink;

import org.apache.paimon.manifest.ManifestCommittable;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;

import java.util.List;

/**
 * A {@link CommittableStateManager} which does nothing. If a commit attempt fails, it will be lost
 * after the job restarts.
 *
 * <p>Useful for committing optional snapshots. For example COMPACT snapshots produced by a separate
 * compact job.
 */
public class NoopCommittableStateManager implements CommittableStateManager<ManifestCommittable> {

    @Override
    public void initializeState(
            StateInitializationContext context, Committer<?, ManifestCommittable> committer)
            throws Exception {
        // nothing to do
    }

    @Override
    public void snapshotState(StateSnapshotContext context, List<ManifestCommittable> committables)
            throws Exception {
        // nothing to do
    }
}
