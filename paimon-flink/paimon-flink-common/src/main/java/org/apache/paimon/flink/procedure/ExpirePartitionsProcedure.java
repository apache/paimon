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
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.partition.PartitionExpireStrategy.createPartitionExpireStrategy;

/** A procedure to expire partitions. */
public class ExpirePartitionsProcedure extends ProcedureBase {
    @Override
    public String identifier() {
        return "expire_partitions";
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "expiration_time",
                        type = @DataTypeHint(value = "STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "timestamp_formatter",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "timestamp_pattern",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "expire_strategy",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "max_expires",
                        type = @DataTypeHint("INTEGER"),
                        isOptional = true)
            })
    public @DataTypeHint("ROW< expired_partitions STRING>") Row[] call(
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
        setTableOptions(
                tableOptions,
                CoreOptions.PARTITION_EXPIRATION_MAX_NUM.key(),
                maxExpires != null ? maxExpires.toString() : null);

        CoreOptions runtimeOptions = CoreOptions.fromMap(tableOptions);

        PartitionExpire partitionExpire =
                new PartitionExpire(
                        runtimeOptions.partitionExpireTime(),
                        Duration.ofMillis(0L),
                        createPartitionExpireStrategy(runtimeOptions, fileStore.partitionType()),
                        fileStore.newScan(),
                        fileStore.newCommit(""),
                        fileStoreTable.catalogEnvironment().partitionHandler(),
                        runtimeOptions.partitionExpireMaxNum());

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

    private void setTableOptions(HashMap<String, String> tableOptions, String key, String value) {
        if (!StringUtils.isNullOrWhitespaceOnly(value)) {
            tableOptions.put(key, value);
        }
    }
}
