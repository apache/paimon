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

package org.apache.paimon.flink.action;

import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SnapshotNotExistException;

import java.util.Map;
import java.util.Objects;

/** Reset consumer action for Flink. */
public class ResetConsumerAction extends TableActionBase {

    private final String consumerId;
    private Long nextSnapshotId;

    protected ResetConsumerAction(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String consumerId) {
        super(warehouse, databaseName, tableName, catalogConfig);
        this.consumerId = consumerId;
    }

    public ResetConsumerAction withNextSnapshotIds(Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
        return this;
    }

    @Override
    public void run() throws Exception {
        FileStoreTable dataTable = (FileStoreTable) table;
        SnapshotManager snapshotManager = dataTable.snapshotManager();
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (nextSnapshotId != null
                && latestSnapshotId != null
                && nextSnapshotId > latestSnapshotId) {
            throw new SnapshotNotExistException(
                    String.format(
                            "The specified snapshot id %s should be smaller than the latest snapshot id %s.",
                            latestSnapshotId));
        }

        ConsumerManager consumerManager =
                new ConsumerManager(
                        dataTable.fileIO(),
                        dataTable.location(),
                        dataTable.snapshotManager().branch());
        if (Objects.isNull(nextSnapshotId)) {
            consumerManager.deleteConsumer(consumerId);
        } else {
            consumerManager.resetConsumer(consumerId, new Consumer(nextSnapshotId));
        }
    }
}
