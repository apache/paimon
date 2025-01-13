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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.partition.PartitionExpireStrategy.createPartitionExpireStrategy;

/** Expire partitions action for Flink. */
public class ExpirePartitionsAction extends TableActionBase {
    private final PartitionExpire partitionExpire;

    public ExpirePartitionsAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String expirationTime,
            String timestampFormatter,
            String timestampPattern,
            String expireStrategy) {
        super(databaseName, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports expire_partitions action. The table type is '%s'.",
                            table.getClass().getName()));
        }

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileStore<?> fileStore = fileStoreTable.store();

        HashMap<String, String> tableOptions = new HashMap<>(fileStore.options().toMap());

        // partition.expiration-time should not be null.
        setTableOptions(tableOptions, CoreOptions.PARTITION_EXPIRATION_TIME.key(), expirationTime);
        Preconditions.checkArgument(
                tableOptions.get(CoreOptions.PARTITION_EXPIRATION_TIME.key()) != null,
                String.format(
                        "%s should not be null", CoreOptions.PARTITION_EXPIRATION_TIME.key()));

        setTableOptions(
                tableOptions, CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), timestampFormatter);
        setTableOptions(
                tableOptions, CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), timestampPattern);
        setTableOptions(
                tableOptions, CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(), expireStrategy);

        CoreOptions runtimeOptions = CoreOptions.fromMap(tableOptions);

        this.partitionExpire =
                new PartitionExpire(
                        runtimeOptions.partitionExpireTime(),
                        Duration.ofMillis(0L),
                        createPartitionExpireStrategy(runtimeOptions, fileStore.partitionType()),
                        fileStore.newScan(),
                        fileStore.newCommit(""),
                        fileStoreTable.catalogEnvironment().partitionHandler(),
                        runtimeOptions.partitionExpireMaxNum());
    }

    private void setTableOptions(HashMap<String, String> tableOptions, String key, String value) {
        if (!StringUtils.isNullOrWhitespaceOnly(value)) {
            tableOptions.put(key, value);
        }
    }

    @Override
    public void run() throws Exception {
        this.partitionExpire.expire(Long.MAX_VALUE);
    }
}
