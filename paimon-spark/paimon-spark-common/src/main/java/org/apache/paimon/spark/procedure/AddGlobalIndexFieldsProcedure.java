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

import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.globalindex.CentroidBitmapFieldAppender;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Locale;
import java.util.UUID;

import static org.apache.paimon.spark.procedure.CreateGlobalIndexProcedure.parseExtraFields;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Appends Bitmap filter fields to existing centroid vector indexes. */
public class AddGlobalIndexFieldsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS = {
        ProcedureParameter.required("table", StringType),
        ProcedureParameter.required("index_column", StringType),
        ProcedureParameter.required("index_type", StringType),
        ProcedureParameter.required("extra_fields", StringType),
        ProcedureParameter.optional("partitions", StringType)
    };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected AddGlobalIndexFieldsProcedure(TableCatalog tableCatalog) {
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
    public String description() {
        return "Add Bitmap fields to existing centroid vector indexes.";
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier identifier = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String column = args.getString(1);
        String indexType = args.getString(2).toLowerCase(Locale.ROOT).trim();
        checkArgument(!args.isNullAt(3), "Specify the extra_fields to add.");
        String extraFields = args.getString(3);
        String partitions = args.isNullAt(4) ? null : args.getString(4);

        return modifySparkTableRefreshingCacheOnFailure(
                identifier,
                sparkTable -> {
                    org.apache.paimon.table.Table t = sparkTable.getTable();
                    checkArgument(t instanceof FileStoreTable, "Expected a FileStoreTable.");
                    FileStoreTable table = (FileStoreTable) t;
                    RowType rowType = table.rowType();
                    checkArgument(
                            rowType.containsField(column), "Column '%s' does not exist.", column);
                    DataField vectorField = rowType.getField(column);
                    PartitionPredicate partitionFilter =
                            SparkProcedureUtils.convertPartitionsToPartitionPredicate(
                                    partitions, table, spark());
                    try {
                        List<DataField> fields =
                                parseExtraFields(extraFields, rowType, vectorField);
                        List<CommitMessage> messages =
                                CentroidBitmapFieldAppender.build(
                                        spark(),
                                        table,
                                        indexType,
                                        vectorField,
                                        fields,
                                        partitionFilter);
                        if (!messages.isEmpty()) {
                            try (TableCommitImpl commit =
                                    table.newCommit(
                                            "global-index-add-fields-" + UUID.randomUUID())) {
                                commit.commit(messages);
                            }
                        }
                        return new InternalRow[] {newInternalRow(true)};
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Failed to add fields to %s index on column '%s' in table '%s'.",
                                        indexType, column, identifier),
                                e);
                    }
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<AddGlobalIndexFieldsProcedure>() {
            @Override
            public AddGlobalIndexFieldsProcedure doBuild() {
                return new AddGlobalIndexFieldsProcedure(tableCatalog());
            }
        };
    }
}
