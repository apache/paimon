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

import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.operation.LocalOrphanFilesClean;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.TagManager;

import java.time.Duration;

/** Utils for {@link FileStoreTable}. */
public class FileStoreTableUtils {

    public static void purgeFiles(FileStoreTable table) throws Exception {
        // clear branches
        BranchManager branchManager = table.branchManager();
        branchManager.branches().forEach(branchManager::dropBranch);

        // clear tags
        TagManager tagManager = table.tagManager();
        tagManager.allTagNames().forEach(table::deleteTag);

        // clear consumers
        ConsumerManager consumerManager = table.consumerManager();
        consumerManager.consumers().keySet().forEach(consumerManager::deleteConsumer);

        // truncate table
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncateTable();
        }

        // clear snapshots and changelogs
        ExpireConfig expireConfig =
                ExpireConfig.builder()
                        .changelogMaxDeletes(Integer.MAX_VALUE)
                        .changelogRetainMax(1)
                        .changelogRetainMin(1)
                        .changelogTimeRetain(Duration.ZERO)
                        .snapshotMaxDeletes(Integer.MAX_VALUE)
                        .snapshotRetainMax(1)
                        .snapshotRetainMin(1)
                        .snapshotTimeRetain(Duration.ZERO)
                        .build();
        table.newExpireChangelog().config(expireConfig).expire();
        table.newExpireSnapshots().config(expireConfig).expire();

        // clear orphan files
        LocalOrphanFilesClean clean = new LocalOrphanFilesClean(table, System.currentTimeMillis());
        clean.clean();
    }
}
