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
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.TimeUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A procedure to expire partitions. */
public class ExpirePartitionsProcedure extends ProcedureBase {
    @Override
    public String identifier() {
        return "expire_partitions";
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "expiration_time", type = @DataTypeHint(value = "STRING")),
                @ArgumentHint(
                        name = "timestamp_formatter",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "expire_strategy",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
            })
    public @DataTypeHint("ROW< expired_partitions STRING>") Row[] call(
            ProcedureContext procedureContext,
            String tableId,
            String expirationTime,
            String timestampFormatter,
            String expireStrategy)
            throws Catalog.TableNotExistException {
        FileStoreTable fileStoreTable = (FileStoreTable) table(tableId);
        FileStore fileStore = fileStoreTable.store();
        Map<String, String> map = new HashMap<>();
        map.put(CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(), expireStrategy);
        map.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), timestampFormatter);

        PartitionExpire partitionExpire =
                new PartitionExpire(
                        TimeUtils.parseDuration(expirationTime),
                        Duration.ofMillis(0L),
                        PartitionExpireStrategy.createPartitionExpireStrategy(
                                CoreOptions.fromMap(map), fileStore.partitionType()),
                        fileStore.newScan(),
                        fileStore.newCommit(""),
                        Optional.ofNullable(
                                        fileStoreTable
                                                .catalogEnvironment()
                                                .metastoreClientFactory())
                                .map(MetastoreClient.Factory::create)
                                .orElse(null));
        List<Map<String, String>> expired = partitionExpire.expire(Long.MAX_VALUE);
        return expired == null || expired.isEmpty()
                ? new Row[] {Row.of("No expired partitions.")}
                : expired.stream()
                        .map(
                                x -> {
                                    String r = x.toString();
                                    return Row.of(r.substring(1, r.length() - 1));
                                })
                        .toArray(Row[]::new);
    }
}
