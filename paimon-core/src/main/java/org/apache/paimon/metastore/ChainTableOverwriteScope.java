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

package org.apache.paimon.metastore;

import org.apache.paimon.data.BinaryRow;

import java.util.Collections;
import java.util.Set;

/**
 * Carries the set of delta partitions that a chain-table OVERWRITE freshly (re)wrote from {@link
 * ChainTableOverwriteCommitCallback} to the snapshot-branch truncate's {@link
 * ChainTableCommitPreCallback}.
 *
 * <p>The overwrite callback truncates the snapshot branch synchronously, on the same thread, and
 * that truncate is what invokes the pre-callback. A thread-local scoped around the truncate call
 * therefore reaches the pre-callback without changing the generic commit path or the {@link
 * org.apache.paimon.table.sink.CommitPreCallback} signature.
 *
 * <p>Why the pre-callback needs it: a delta partition that this overwrite just rewrote holds fresh,
 * complete data and does not depend on a snapshot baseline, so dropping that baseline is intended,
 * not an orphan. Only the trigger site knows which partitions were freshly written; a standalone
 * drop or a rollback sets nothing here, so the pre-callback keeps rejecting genuinely stranded
 * followers.
 */
final class ChainTableOverwriteScope {

    private static final ThreadLocal<Set<BinaryRow>> FRESHLY_WRITTEN_DELTA_PARTITIONS =
            new ThreadLocal<>();

    private ChainTableOverwriteScope() {}

    static void setFreshlyWrittenDeltaPartitions(Set<BinaryRow> partitions) {
        FRESHLY_WRITTEN_DELTA_PARTITIONS.set(partitions);
    }

    static void clear() {
        FRESHLY_WRITTEN_DELTA_PARTITIONS.remove();
    }

    static Set<BinaryRow> freshlyWrittenDeltaPartitions() {
        Set<BinaryRow> partitions = FRESHLY_WRITTEN_DELTA_PARTITIONS.get();
        return partitions == null ? Collections.emptySet() : partitions;
    }
}
