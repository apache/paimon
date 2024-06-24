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

import org.apache.paimon.FileStore;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.TimeUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.Duration;
import java.util.Optional;

import static org.apache.spark.sql.types.DataTypes.StringType;

/** A procedure to expire partitions. */
public class ExpirePartitionsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("expiration_time", StringType),
                ProcedureParameter.optional("timestamp_formatter", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
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
        String timestampFormatter = args.getString(2);
        return modifyPaimonTable(
                tableIdent,
                table -> {
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    FileStore fileStore = fileStoreTable.store();
                    PartitionExpire partitionExpire =
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
                    partitionExpire.expire(Long.MAX_VALUE);
                    InternalRow outputRow = newInternalRow(true);
                    return new InternalRow[] {outputRow};
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

    @Override
    public String description() {
        return "ExpirePartitionsProcedure";
    }
}
