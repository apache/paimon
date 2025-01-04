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
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.PartitionPathUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import static org.apache.paimon.utils.ParameterUtils.getPartitions;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Partition mark done procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.mark_partition_done('tableId', 'partition1;partition2')
 * </code></pre>
 */
public class MarkPartitionDoneProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("partitions", StringType)
            };

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected MarkPartitionDoneProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String partitionStr = args.getString(1);
        String[] partitions = partitionStr.split(";");

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    checkArgument(
                            table instanceof FileStoreTable,
                            "Only FileStoreTable supports mark_partition_done procedure. The table type is '%s'.",
                            table.getClass().getName());

                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    CoreOptions coreOptions = fileStoreTable.coreOptions();
                    List<PartitionMarkDoneAction> actions =
                            PartitionMarkDoneAction.createActions(
                                    getClass().getClassLoader(), fileStoreTable, coreOptions);

                    List<String> partitionPaths =
                            PartitionPathUtils.generatePartitionPaths(
                                    getPartitions(partitions),
                                    fileStoreTable.store().partitionType());

                    markDone(partitionPaths, actions);

                    IOUtils.closeAllQuietly(actions);
                    InternalRow outputRow = newInternalRow(true);
                    return new InternalRow[] {outputRow};
                });
    }

    public static void markDone(List<String> partitions, List<PartitionMarkDoneAction> actions) {
        for (String partition : partitions) {
            try {
                for (PartitionMarkDoneAction action : actions) {
                    action.markDone(partition);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<MarkPartitionDoneProcedure>() {
            @Override
            public MarkPartitionDoneProcedure doBuild() {
                return new MarkPartitionDoneProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "MarkPartitionDoneProcedure";
    }
}
