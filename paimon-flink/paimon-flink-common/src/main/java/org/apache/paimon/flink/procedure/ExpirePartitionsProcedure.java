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

import org.apache.paimon.FileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

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
                        isOptional = true),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public @DataTypeHint("ROW< expired_partitions STRING>") Row[] call(
            ProcedureContext procedureContext,
            String tableId,
            String expirationTime,
            String timestampFormatter,
            String timestampPattern,
            String expireStrategy,
            Integer maxExpires,
            String options)
            throws Catalog.TableNotExistException {
        Map<String, String> dynamicOptions =
                ProcedureUtils.fillInPartitionOptions(
                        expireStrategy,
                        timestampFormatter,
                        timestampPattern,
                        expirationTime,
                        maxExpires,
                        options);
        Table table = table(tableId).copy(dynamicOptions);
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileStore fileStore = fileStoreTable.store();

        PartitionExpire partitionExpire =
                fileStore.newPartitionExpire(fileStore.options().createCommitUser());
        Preconditions.checkNotNull(
                partitionExpire,
                "Both the partition expiration time and partition field can not be null.");

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
