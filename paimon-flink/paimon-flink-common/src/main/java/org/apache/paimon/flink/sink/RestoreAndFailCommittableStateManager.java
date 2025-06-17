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

import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.utils.SerializableSupplier;

import java.util.List;

/**
 * A {@link CommittableStateManager} which stores uncommitted {@link ManifestCommittable}s in state.
 *
 * <p>When the job restarts, these {@link ManifestCommittable}s will be restored and committed, then
 * an intended failure will occur, hoping that after the job restarts, all writers can start writing
 * based on the restored snapshot.
 *
 * <p>Useful for committing snapshots containing records. For example snapshots produced by table
 * store writers.
 */
public class RestoreAndFailCommittableStateManager<GlobalCommitT>
        extends RestoreCommittableStateManager<GlobalCommitT> {

    private static final long serialVersionUID = 1L;

    public RestoreAndFailCommittableStateManager(
            SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer,
            boolean partitionMarkDoneRecoverFromState) {
        super(committableSerializer, partitionMarkDoneRecoverFromState);
    }

    @Override
    protected int recover(List<GlobalCommitT> committables, Committer<?, GlobalCommitT> committer)
            throws Exception {
        int numCommitted = super.recover(committables, committer);
        if (numCommitted > 0) {
            throw new RuntimeException(
                    "This exception is intentionally thrown "
                            + "after committing the restored checkpoints. "
                            + "By restarting the job we hope that "
                            + "writers can start writing based on these new commits.");
        }
        return numCommitted;
    }
}
