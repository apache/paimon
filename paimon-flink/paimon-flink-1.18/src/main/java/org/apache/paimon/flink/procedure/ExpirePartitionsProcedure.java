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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.TimeUtils;

import org.apache.flink.table.procedure.ProcedureContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.partition.PartitionExpireStrategy.createPartitionExpireStrategy;

/** A procedure to expire partitions. */
public class ExpirePartitionsProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "expire_partitions";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String expirationTime,
            String timestampFormatter,
            String timestampPattern,
            String expireStrategy)
            throws Catalog.TableNotExistException {
        return call(
                procedureContext,
                tableId,
                expirationTime,
                timestampFormatter,
                timestampPattern,
                expireStrategy,
                null);
    }

    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String expirationTime,
            String timestampFormatter,
            String timestampPattern,
            String expireStrategy,
            Integer maxExpires)
            throws Catalog.TableNotExistException {
        FileStoreTable fileStoreTable = (FileStoreTable) table(tableId);
        FileStore fileStore = fileStoreTable.store();
        Map<String, String> map = new HashMap<>();
        map.put(CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(), expireStrategy);
        map.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), timestampFormatter);
        map.put(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), timestampPattern);

        PartitionExpire partitionExpire =
                new PartitionExpire(
                        TimeUtils.parseDuration(expirationTime),
                        Duration.ofMillis(0L),
                        createPartitionExpireStrategy(
                                CoreOptions.fromMap(map), fileStore.partitionType()),
                        fileStore.newScan(),
                        fileStore.newCommit(""),
                        Optional.ofNullable(
                                        fileStoreTable
                                                .catalogEnvironment()
                                                .metastoreClientFactory())
                                .map(MetastoreClient.Factory::create)
                                .orElse(null),
                        fileStore.options().partitionExpireMaxNum());
        if (maxExpires != null) {
            partitionExpire.withMaxExpireNum(maxExpires);
        }
        List<Map<String, String>> expired = partitionExpire.expire(Long.MAX_VALUE);
        return expired == null || expired.isEmpty()
                ? new String[] {"No expired partitions."}
                : expired.stream()
                        .map(
                                x -> {
                                    String r = x.toString();
                                    return r.substring(1, r.length() - 1);
                                })
                        .toArray(String[]::new);
    }
}
