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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.MetricRegistry;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** Inner {@link TableCommit} contains overwrite setter. */
public interface InnerTableCommit extends StreamTableCommit, BatchTableCommit {

    /** Overwrite writing, same as the 'INSERT OVERWRITE T PARTITION (...)' semantics of SQL. */
    InnerTableCommit withOverwrite(@Nullable Map<String, String> spec);

    /**
     * Overwrite specified partitions. Unlike {@link InnerTableCommit#withOverwrite}, the given
     * partitions must have all the partition keys.
     */
    InnerTableCommit withOverwriteStaticPartitions(List<BinaryRow> staticPartitions);

    /**
     * If this is set to true, when there is no new data, no snapshot will be generated. By default,
     * empty commit is ignored.
     *
     * <ul>
     *   <li>For Streaming: the default value of 'ignoreEmptyCommit' is false.
     *   <li>For Batch: the default value of 'ignoreEmptyCommit' is true.
     * </ul>
     *
     * <p>If there are no new files or compact files at the same time, no new commit will be
     * generated regardless of the configuration (No one trigger commit interface).
     */
    InnerTableCommit ignoreEmptyCommit(boolean ignoreEmptyCommit);

    InnerTableCommit expireForEmptyCommit(boolean expireForEmptyCommit);

    /**
     * If this is set to true, {@link StreamTableCommit#filterAndCommit} verifies that every file it
     * is about to commit still exists. By default it does.
     *
     * <p>The check guards a committable that was restored from an engine's state and may reference
     * files deleted long ago. A caller which filters a committable it has just produced itself
     * knows those files exist, and can skip a file listing proportional to the size of the
     * committable.
     */
    InnerTableCommit checkFilesExistence(boolean checkFilesExistence);

    /**
     * If this is set to true, maintenance runs on the committing thread and its failure is thrown
     * to the caller, instead of running through an executor which stores the failure for the next
     * commit to report.
     *
     * <p>A committer which commits once and is then closed has to do this: it is about to shut the
     * executor down, so maintenance dispatched to it may never run, and there is no next commit to
     * report a failure to. {@link BatchTableCommit#commit(List)} already behaves this way; a caller
     * which commits through {@link StreamTableCommit#filterAndCommit} with the same one-shot
     * lifecycle has to ask for it.
     */
    InnerTableCommit inlineMaintenance(boolean inlineMaintenance);

    InnerTableCommit appendCommitCheckConflict(boolean appendCommitCheckConflict);

    InnerTableCommit rowIdCheckConflict(@Nullable Long rowIdCheckFromSnapshot);

    InnerTableCommit rowIdCheckConflictForMaterializeDvCompaction(
            @Nullable Long rowIdCheckFromSnapshot);

    @Override
    InnerTableCommit withMetricRegistry(MetricRegistry registry);
}
