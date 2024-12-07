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
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.ParameterUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.procedure.ProcedureContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                null,
                null);
    }

    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String expirationTime,
            String timestampFormatter,
            String timestampPattern,
            String expireStrategy,
            Integer maxExpires,
            String options)
            throws Catalog.TableNotExistException {
        Map<String, String> dynamicOptions = new HashMap<>();
        if (!StringUtils.isNullOrWhitespaceOnly(expireStrategy)) {
            dynamicOptions.put(CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(), expireStrategy);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(timestampFormatter)) {
            dynamicOptions.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), timestampFormatter);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(timestampPattern)) {
            dynamicOptions.put(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), timestampPattern);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(expirationTime)) {
            dynamicOptions.put(CoreOptions.PARTITION_EXPIRATION_TIME.key(), expirationTime);
        }
        if (maxExpires != null) {
            dynamicOptions.put(
                    CoreOptions.PARTITION_EXPIRATION_MAX_NUM.key(), String.valueOf(maxExpires));
        }
        if (!StringUtils.isNullOrWhitespaceOnly(options)) {
            dynamicOptions.putAll(ParameterUtils.parseCommaSeparatedKeyValues(options));
        }

        Table table = table(tableId).copy(dynamicOptions);
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileStore fileStore = fileStoreTable.store();

        // check expiration time not null
        Preconditions.checkNotNull(
                fileStore.options().partitionExpireTime(),
                "The partition expiration time is must been required, you can set it by configuring the property 'partition.expiration-time' or adding the 'expiration_time' parameter in procedure.  ");

        PartitionExpire partitionExpire =
                new PartitionExpire(
                        fileStore.options().partitionExpireTime(),
                        Duration.ofMillis(0L),
                        createPartitionExpireStrategy(
                                fileStore.options(), fileStore.partitionType()),
                        fileStore.newScan(),
                        fileStore.newCommit(""),
                        fileStoreTable.catalogEnvironment().partitionHandler(),
                        fileStore.options().partitionExpireMaxNum());

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
