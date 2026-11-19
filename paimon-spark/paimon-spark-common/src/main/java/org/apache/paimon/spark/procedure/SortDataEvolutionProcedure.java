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

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionRangeSortPlan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Range;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.paimon.shims.SparkShimLoader;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Sorts a complete row-ID range per partition and assigns new row IDs to its surviving rows. */
public class SortDataEvolutionProcedure extends BaseProcedure {
    private static final ProcedureParameter[] PARAMETERS = {
        ProcedureParameter.required("table", StringType),
        ProcedureParameter.required("range", StringType),
        ProcedureParameter.required("order_by", StringType),
        ProcedureParameter.optional("where", StringType)
    };

    private SortDataEvolutionProcedure(TableCatalog catalog) {
        super(catalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return new StructType().add("rewritten_partitions", IntegerType, false);
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier ident = toIdentifier(args.getString(0), "table");
        Range range =
                DataEvolutionRangeSortPlan.parseRange(args.isNullAt(1) ? null : args.getString(1));
        checkArgument(!args.isNullAt(2), "The order_by argument is required.");
        List<String> fields = DataEvolutionRangeSortPlan.parseSortFields(args.getString(2));
        String where = args.isNullAt(3) ? null : args.getString(3);
        return modifySparkTableRefreshingCacheOnFailure(
                ident,
                sparkTable -> {
                    checkArgument(
                            sparkTable.getTable() instanceof FileStoreTable,
                            "Expected a file store table.");
                    FileStoreTable table = (FileStoreTable) sparkTable.getTable();
                    DataEvolutionRangeSortPlan.validate(table, range, fields);
                    PartitionPredicate filter =
                            SparkProcedureUtils.convertToPartitionPredicate(
                                    where,
                                    table.schema().logicalPartitionType(),
                                    spark(),
                                    createRelation(ident, sparkTable));
                    Snapshot snapshot = table.snapshotManager().latestSnapshot();
                    if (snapshot == null) {
                        return new InternalRow[] {newInternalRow(0)};
                    }
                    List<DataEvolutionRangeSortPlan> plans =
                            DataEvolutionRangeSortPlan.plan(table, snapshot, range, filter, fields);
                    if (!plans.isEmpty()) {
                        SparkDataEvolutionRangeSort.execute(
                                table, snapshot, fields, plans, spark());
                    }
                    return new InternalRow[] {newInternalRow(plans.size())};
                });
    }

    @Override
    protected void refreshSparkCache(Identifier ident, Table table) {
        super.refreshSparkCache(ident, table);
        // Spark analyzes queries reading _ROW_ID with a separate metadata-bearing relation.
        SparkShimLoader.shim()
                .classicApi()
                .recacheByPlan(spark(), createRelation(ident, table).withMetadataColumns());
    }

    @Override
    public String description() {
        return "Sort complete Data Evolution file groups within an inclusive row ID range.";
    }

    public static ProcedureBuilder builder() {
        return new Builder<SortDataEvolutionProcedure>() {
            @Override
            public SortDataEvolutionProcedure doBuild() {
                return new SortDataEvolutionProcedure(tableCatalog());
            }
        };
    }
}
