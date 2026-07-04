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

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.globalindex.GlobalIndexTopologyBuilder;
import org.apache.paimon.spark.globalindex.GlobalIndexTopologyBuilderUtils;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProcedureUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Procedure to build global index files via Spark. */
public class CreateGlobalIndexProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(CreateGlobalIndexProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", DataTypes.StringType),
                ProcedureParameter.required("index_column", DataTypes.StringType),
                ProcedureParameter.required("index_type", DataTypes.StringType),
                ProcedureParameter.optional("partitions", StringType),
                ProcedureParameter.optional("options", DataTypes.StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected CreateGlobalIndexProcedure(TableCatalog tableCatalog) {
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
        return "Create global index files for a given column.";
    }

    static Options createUserOptions(FileStoreTable table, String optionString) {
        return createUserOptions(table.options(), optionString);
    }

    static Options createUserOptions(Map<String, String> tableOptions, String optionString) {
        HashMap<String, String> parsedOptions = new HashMap<>();
        ProcedureUtils.putAllOptions(parsedOptions, optionString);
        return new Options(tableOptions, parsedOptions);
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String column = args.getString(1);
        String indexType = args.getString(2).toLowerCase(Locale.ROOT).trim();
        String partitions =
                (args.isNullAt(3) || StringUtils.isNullOrWhitespaceOnly(args.getString(3)))
                        ? null
                        : args.getString(3);
        String optionString = args.isNullAt(4) ? null : args.getString(4);

        LOG.info(
                "Starting to build index for table {} with partitions: {}", tableIdent, partitions);

        return modifySparkTable(
                tableIdent,
                sparkTable -> {
                    List<String> indexColumns =
                            Arrays.stream(column.split(","))
                                    .map(String::trim)
                                    .filter(s -> !s.isEmpty())
                                    .collect(Collectors.toList());
                    try {
                        org.apache.paimon.table.Table t = sparkTable.getTable();
                        checkArgument(
                                t instanceof FileStoreTable,
                                "Only FileStoreTable supports global index creation.");
                        FileStoreTable table = (FileStoreTable) t;
                        checkArgument(
                                table.coreOptions().rowTrackingEnabled(),
                                "Table '%s' must enable 'row-tracking.enabled=true' before creating global index.",
                                tableIdent);

                        RowType rowType = table.rowType();
                        checkArgument(!indexColumns.isEmpty(), "At least one column required.");
                        checkArgument(
                                indexColumns.size() == new HashSet<>(indexColumns).size(),
                                "Duplicate index columns are not allowed: %s",
                                indexColumns);
                        // No hard cap on the number of index columns: unlike row-store B-tree
                        // indexes (e.g. MySQL 16, PostgreSQL 32) whose limit comes from composing
                        // columns into a single key, the global index is built on per-type index
                        // frameworks. Whether multiple columns are supported, and any practical
                        // limit, is decided by each index type (single-column types reject
                        // multi-column via UnsupportedOperationException).
                        for (String col : indexColumns) {
                            checkArgument(
                                    rowType.containsField(col),
                                    "Column '%s' does not exist in table '%s'.",
                                    col,
                                    tableIdent);
                        }
                        DataSourceV2Relation relation = createRelation(tableIdent, sparkTable);
                        PartitionPredicate partitionPredicate =
                                SparkProcedureUtils.convertPartitionsToPartitionPredicate(
                                        partitions, table, spark());

                        List<DataField> indexFields =
                                indexColumns.stream()
                                        .map(rowType::getField)
                                        .collect(Collectors.toList());
                        RowType projectedRowType = rowType.project(indexColumns);
                        RowType readRowType = SpecialFields.rowTypeWithRowId(projectedRowType);

                        Options userOptions = createUserOptions(table, optionString);

                        if (indexColumns.size() > 1) {
                            // Fail fast before submitting the job: index types that do not support
                            // multi-column throw from GlobalIndexerFactory#create, which happens
                            // before any indexer side effect.
                            try {
                                GlobalIndexer.create(
                                        indexType,
                                        indexFields.get(0),
                                        indexFields.subList(1, indexFields.size()),
                                        userOptions);
                            } catch (UnsupportedOperationException e) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Index type '%s' does not support multi-column index, got columns: %s",
                                                indexType, indexColumns));
                            }
                        }

                        GlobalIndexTopologyBuilder topoBuilder =
                                GlobalIndexTopologyBuilderUtils.createTopoBuilder(indexType);

                        List<CommitMessage> indexResults =
                                topoBuilder.buildIndex(
                                        spark(),
                                        relation,
                                        partitionPredicate,
                                        table,
                                        indexType,
                                        readRowType,
                                        indexFields.get(0),
                                        indexFields.subList(1, indexFields.size()),
                                        userOptions);

                        try (TableCommitImpl commit =
                                table.newCommit(
                                        "global-index-"
                                                + indexType
                                                + "-create-"
                                                + UUID.randomUUID())) {
                            commit.commit(indexResults);
                        }

                        return new InternalRow[] {newInternalRow(true)};
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Failed to create %s index for columns '%s' on table '%s'.",
                                        indexType, indexColumns, tableIdent),
                                e);
                    }
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<CreateGlobalIndexProcedure>() {
            @Override
            public CreateGlobalIndexProcedure doBuild() {
                return new CreateGlobalIndexProcedure(tableCatalog());
            }
        };
    }
}
