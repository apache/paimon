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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.TimeUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.partition.PartitionExpireStrategy.createPartitionExpireStrategy;

/** Expire partitions action for Flink. */
public class ExpirePartitionsAction extends TableActionBase implements LocalAction {
    private final String expirationTime;
    private final Map<String, String> map;

    public ExpirePartitionsAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig,
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
        table = table.copy(tableConfig);
        this.expirationTime = expirationTime;
        map = new HashMap<>();
        map.put(CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(), expireStrategy);
        map.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), timestampFormatter);
        map.put(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), timestampPattern);
    }

    @Override
    public void executeLocally() throws Exception {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileStore<?> fileStore = fileStoreTable.store();
        PartitionExpire partitionExpire =
                fileStore.newPartitionExpire(
                        "",
                        fileStoreTable,
                        TimeUtils.parseDuration(expirationTime),
                        Duration.ofMillis(0L),
                        createPartitionExpireStrategy(
                                CoreOptions.fromMap(map),
                                fileStore.partitionType(),
                                catalogLoader(),
                                new Identifier(
                                        identifier.getDatabaseName(), identifier.getTableName())));

        partitionExpire.expire(Long.MAX_VALUE);
    }
}
