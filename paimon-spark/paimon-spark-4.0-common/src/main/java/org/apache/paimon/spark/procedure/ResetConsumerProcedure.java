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

import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Reset consumer procedure. Usage:
 *
 * <pre><code>
 *  -- reset the new next snapshot id in the consumer
 *  CALL sys.reset_consumer('tableId', 'consumerId', nextSnapshotId)
 *
 *  -- delete consumer
 *  CALL sys.reset_consumer('tableId', 'consumerId')
 * </code></pre>
 */
public class ResetConsumerProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("consumerId", StringType),
                ProcedureParameter.optional("nextSnapshotId", LongType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected ResetConsumerProcedure(TableCatalog tableCatalog) {
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
        String consumerId = args.getString(1);
        Long nextSnapshotId = args.isNullAt(2) ? null : args.getLong(2);
        return modifyPaimonTable(
                tableIdent,
                table -> {
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    ConsumerManager consumerManager =
                            new ConsumerManager(fileStoreTable.fileIO(), fileStoreTable.location());
                    if (nextSnapshotId == null) {
                        consumerManager.deleteConsumer(consumerId);
                    } else {
                        consumerManager.resetConsumer(consumerId, new Consumer(nextSnapshotId));
                    }

                    InternalRow outputRow = newInternalRow(true);
                    return new InternalRow[] {outputRow};
                });
    }

    public static ProcedureBuilder builder() {
        return new Builder<ResetConsumerProcedure>() {
            @Override
            public ResetConsumerProcedure doBuild() {
                return new ResetConsumerProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ResetConsumerProcedure";
    }
}
