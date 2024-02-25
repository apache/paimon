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

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;

import java.io.Serializable;
import java.util.List;

/**
 * Helper interface for {@link CommitterOperator}. This interface manages operator states about
 * {@link org.apache.paimon.manifest.ManifestCommittable}.
 */
public interface CommittableStateManager<GlobalCommitT> extends Serializable {

    void initializeState(StateInitializationContext context, Committer<?, GlobalCommitT> committer)
            throws Exception;

    void snapshotState(StateSnapshotContext context, List<GlobalCommitT> committables)
            throws Exception;
}
