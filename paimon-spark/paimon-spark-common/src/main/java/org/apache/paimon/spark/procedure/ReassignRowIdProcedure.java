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

import org.apache.paimon.append.dataevolution.DataEvolutionRowIdReassigner;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Reassign row id procedure. Reassigns row IDs for a data evolution table when partition row-id
 * ranges overlap. Usage:
 *
 * <pre><code>
 *  CALL sys.reassign_row_id(table => 'default.T')
 *  CALL sys.reassign_row_id(table => 'default.T', partitions => 'dt=2026-05-19')
 * </code></pre>
 */
public class ReassignRowIdProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("partitions", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", StringType, true, Metadata.empty())
                    });

    protected ReassignRowIdProcedure(TableCatalog tableCatalog) {
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
        String partitions = args.isNullAt(1) ? null : args.getString(1);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    checkArgument(
                            table instanceof FileStoreTable,
                            "Only FileStoreTable supports reassign_row_id procedure. The table type is '%s'.",
                            table.getClass().getName());

                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    PartitionPredicate partitionPredicate =
                            SparkProcedureUtils.convertPartitionsToPartitionPredicate(
                                    partitions, fileStoreTable, spark());

                    DataEvolutionRowIdReassigner.Result result =
                            new DataEvolutionRowIdReassigner(fileStoreTable, partitionPredicate)
                                    .reassign();
                    String message = formatResult(tableIdent.toString(), result);
                    return new InternalRow[] {newInternalRow(UTF8String.fromString(message))};
                });
    }

    private static String formatResult(
            String tableName, DataEvolutionRowIdReassigner.Result result) {
        if (!result.reassigned) {
            String reason =
                    result.skipReason == null
                            ? "row IDs are already partition-contiguous"
                            : result.skipReason;
            return String.format(
                    "Skipped. Row IDs for table '%s' were not reassigned because %s: "
                            + "snapshot %d unchanged, nextRowId=%d.",
                    tableName, reason, result.previousSnapshotId, result.nextRowId);
        }
        return String.format(
                "Success. Reassigned row IDs for table '%s': snapshot %d -> %d, "
                        + "nextRowId %d -> %d, files=%d, rows=%d, indexFiles=%d.",
                tableName,
                result.previousSnapshotId,
                result.newSnapshotId,
                result.firstAssignedRowId,
                result.nextRowId,
                result.fileCount,
                result.rowCount,
                result.indexFileCount);
    }

    public static ProcedureBuilder builder() {
        return new Builder<ReassignRowIdProcedure>() {
            @Override
            public ReassignRowIdProcedure doBuild() {
                return new ReassignRowIdProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ReassignRowIdProcedure";
    }
}
