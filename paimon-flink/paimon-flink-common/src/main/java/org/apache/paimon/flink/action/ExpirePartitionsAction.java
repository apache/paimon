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

import org.apache.paimon.FileStore;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.TimeUtils;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Expire partitions action for Flink. */
public class ExpirePartitionsAction extends TableActionBase {

    private final PartitionExpire partitionExpire;

    public ExpirePartitionsAction(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String expirationTime,
            String timestampFormatter) {
        super(warehouse, databaseName, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports expire_partitions action. The table type is '%s'.",
                            table.getClass().getName()));
        }

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileStore<?> fileStore = fileStoreTable.store();
        this.partitionExpire =
                new PartitionExpire(
                        fileStore.partitionType(),
                        TimeUtils.parseDuration(expirationTime),
                        Duration.ofMillis(0L),
                        null,
                        timestampFormatter,
                        fileStore.newScan(),
                        fileStore.newCommit(""),
                        Optional.ofNullable(
                                        fileStoreTable
                                                .catalogEnvironment()
                                                .metastoreClientFactory())
                                .map(MetastoreClient.Factory::create)
                                .orElse(null));
    }

    @Override
    public void run() throws Exception {
        this.partitionExpire.expire(Long.MAX_VALUE);
    }
}
