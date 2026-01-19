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

package org.apache.paimon.table;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.expire.ExpireSnapshotsExecutor;
import org.apache.paimon.operation.expire.ExpireSnapshotsPlan;
import org.apache.paimon.operation.expire.ExpireSnapshotsPlanner;
import org.apache.paimon.operation.expire.SnapshotExpireTask;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * An implementation for {@link ExpireSnapshots}.
 *
 * <p>This implementation uses serial execution mode, which is suitable for local execution. The
 * execution follows four phases:
 *
 * <ul>
 *   <li>Phase 1a: Delete all data files (using dataFileTasks)
 *   <li>Phase 1b: Delete all changelog files (using changelogFileTasks)
 *   <li>Phase 2a: Delete all manifests (using manifestTasks)
 *   <li>Phase 2b: Delete all snapshot files (using snapshotFileTasks)
 * </ul>
 */
public class ExpireSnapshotsImpl implements ExpireSnapshots {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsImpl.class);

    private final ExpireSnapshotsPlanner planner;
    private final ExpireSnapshotsExecutor executor;

    private ExpireConfig expireConfig;

    public ExpireSnapshotsImpl(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            SnapshotDeletion snapshotDeletion,
            TagManager tagManager) {
        this.expireConfig = ExpireConfig.builder().build();

        ConsumerManager consumerManager =
                new ConsumerManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());
        this.planner =
                new ExpireSnapshotsPlanner(
                        snapshotManager, consumerManager, snapshotDeletion, tagManager);
        this.executor =
                new ExpireSnapshotsExecutor(snapshotManager, snapshotDeletion, changelogManager);
    }

    @Override
    public ExpireSnapshots config(ExpireConfig expireConfig) {
        this.expireConfig = expireConfig;
        return this;
    }

    @Override
    public int expire() {
        ExpireSnapshotsPlan plan = planner.plan(expireConfig);
        if (plan.isEmpty()) {
            return 0;
        }

        return executeWithPlan(plan);
    }

    @VisibleForTesting
    public int executeWithPlan(ExpireSnapshotsPlan plan) {
        long startTime = System.currentTimeMillis();

        // Phase 1a: Delete all data files
        for (SnapshotExpireTask task : plan.dataFileTasks()) {
            executor.execute(task, plan.protectionSet().taggedSnapshots(), null);
        }

        // Phase 1b: Delete all changelog files
        for (SnapshotExpireTask task : plan.changelogFileTasks()) {
            executor.execute(task, null, null);
        }

        // Clean empty directories after all data files and changelog files are deleted
        executor.cleanEmptyDirectories();

        // Phase 2a: Delete all manifests
        if (plan.protectionSet().manifestSkippingSet() != null) {
            Set<String> skippingSet = new HashSet<>(plan.protectionSet().manifestSkippingSet());
            for (SnapshotExpireTask task : plan.manifestTasks()) {
                executor.execute(task, null, skippingSet);
            }
        }

        // Phase 2b: Delete all snapshot files
        for (SnapshotExpireTask task : plan.snapshotFileTasks()) {
            executor.execute(task, null, null);
        }

        // Write earliest hint
        executor.writeEarliestHint(plan.endExclusiveId());

        long duration = System.currentTimeMillis() - startTime;
        LOG.info(
                "Finished expire snapshots, duration {} ms, range is [{}, {})",
                duration,
                plan.beginInclusiveId(),
                plan.endExclusiveId());

        return (int) (plan.endExclusiveId() - plan.beginInclusiveId());
    }
}
