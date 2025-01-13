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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.partition.PartitionExpireStrategy.createPartitionExpireStrategy;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** A procedure to expire partitions. */
public class ExpirePartitionsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("expiration_time", StringType),
                ProcedureParameter.optional("timestamp_formatter", StringType),
                ProcedureParameter.optional("timestamp_pattern", StringType),
                ProcedureParameter.optional("expire_strategy", StringType),
                ProcedureParameter.optional("max_expires", IntegerType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("expired_partitions", StringType, true, Metadata.empty())
                    });

    protected ExpirePartitionsProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String expirationTime = args.getString(1);
        String timestampFormatter = args.isNullAt(2) ? null : args.getString(2);
        String timestampPattern = args.isNullAt(3) ? null : args.getString(3);
        String expireStrategy = args.isNullAt(4) ? null : args.getString(4);
        Integer maxExpires = args.isNullAt(5) ? null : args.getInt(5);
        return modifyPaimonTable(
                tableIdent,
                table -> {
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    FileStore fileStore = fileStoreTable.store();

                    HashMap<String, String> tableOptions =
                            new HashMap<>(fileStore.options().toMap());
                    // partition.expiration-time should not be null.
                    setTableOptions(
                            tableOptions,
                            CoreOptions.PARTITION_EXPIRATION_TIME.key(),
                            expirationTime);
                    Preconditions.checkArgument(
                            tableOptions.get(CoreOptions.PARTITION_EXPIRATION_TIME.key()) != null,
                            String.format(
                                    "%s should not be null",
                                    CoreOptions.PARTITION_EXPIRATION_TIME.key()));

                    setTableOptions(
                            tableOptions,
                            CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(),
                            timestampFormatter);
                    setTableOptions(
                            tableOptions,
                            CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(),
                            timestampPattern);
                    setTableOptions(
                            tableOptions,
                            CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(),
                            expireStrategy);
                    setTableOptions(
                            tableOptions,
                            CoreOptions.PARTITION_EXPIRATION_MAX_NUM.key(),
                            maxExpires != null ? maxExpires.toString() : null);

                    CoreOptions runtimeOptions = CoreOptions.fromMap(tableOptions);

                    PartitionExpire partitionExpire =
                            new PartitionExpire(
                                    runtimeOptions.partitionExpireTime(),
                                    Duration.ofMillis(0L),
                                    createPartitionExpireStrategy(
                                            runtimeOptions, fileStore.partitionType()),
                                    fileStore.newScan(),
                                    fileStore.newCommit(""),
                                    fileStoreTable.catalogEnvironment().partitionHandler(),
                                    runtimeOptions.partitionExpireMaxNum());

                    List<Map<String, String>> expired = partitionExpire.expire(Long.MAX_VALUE);
                    return expired == null || expired.isEmpty()
                            ? new InternalRow[] {
                                newInternalRow(UTF8String.fromString("No expired partitions."))
                            }
                            : expired.stream()
                                    .map(
                                            x -> {
                                                String r = x.toString();
                                                return newInternalRow(
                                                        UTF8String.fromString(
                                                                r.substring(1, r.length() - 1)));
                                            })
                                    .toArray(InternalRow[]::new);
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ExpirePartitionsProcedure>() {
            @Override
            public ExpirePartitionsProcedure doBuild() {
                return new ExpirePartitionsProcedure(tableCatalog());
            }
        };
    }

    private void setTableOptions(HashMap<String, String> tableOptions, String key, String value) {
        if (!StringUtils.isNullOrWhitespaceOnly(value)) {
            tableOptions.put(key, value);
        }
    }

    @Override
    public String description() {
        return "ExpirePartitionsProcedure";
    }
}
