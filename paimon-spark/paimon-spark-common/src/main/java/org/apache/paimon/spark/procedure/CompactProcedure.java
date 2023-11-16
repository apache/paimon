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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.options.Options;
import org.apache.paimon.spark.DynamicOverWrite$;
import org.apache.paimon.spark.commands.WriteIntoPaimonTable;
import org.apache.paimon.spark.sort.TableSorter;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ParameterUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.spark.sql.types.DataTypes.StringType;

/** Compact procedure for tables. */
public class CompactProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("partitions", StringType),
                ProcedureParameter.optional("order_strategy", StringType),
                ProcedureParameter.optional("order_by", StringType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected CompactProcedure(TableCatalog tableCatalog) {
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
        Preconditions.checkArgument(args.numFields() >= 1);
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String partitionFilter = blank(args, 1) ? null : toWhere(args.getString(1));
        String sortType = blank(args, 2) ? TableSorter.OrderType.NONE.name() : args.getString(2);
        List<String> sortColumns =
                blank(args, 3)
                        ? Collections.emptyList()
                        : Arrays.asList(args.getString(3).split(","));
        if (TableSorter.OrderType.NONE.name().equals(sortType) && !sortColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "order_strategy \"none\" cannot work with order_by columns.");
        }

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    Preconditions.checkArgument(table instanceof FileStoreTable);
                    InternalRow internalRow =
                            newInternalRow(
                                    execute(
                                            (FileStoreTable) table,
                                            sortType,
                                            sortColumns,
                                            partitionFilter));
                    return new InternalRow[] {internalRow};
                });
    }

    @Override
    public String description() {
        return "This procedure execute sort compact action on unaware-bucket table.";
    }

    private boolean blank(InternalRow args, int index) {
        return args.isNullAt(index) || StringUtils.isBlank(args.getString(index));
    }

    private boolean execute(
            FileStoreTable table,
            String sortType,
            List<String> sortColumns,
            @Nullable String filter) {
        CoreOptions coreOptions = table.store().options();

        // sort only works with bucket=-1 yet
        if (!TableSorter.OrderType.of(sortType).equals(TableSorter.OrderType.NONE)) {
            if (!(table instanceof AppendOnlyFileStoreTable) || coreOptions.bucket() != -1) {
                throw new UnsupportedOperationException(
                        "Spark compact with sort_type "
                                + sortType
                                + " only support unaware-bucket append-only table yet.");
            }
        }

        Dataset<Row> row = spark().read().format("paimon").load(coreOptions.path().getPath());
        row = StringUtils.isBlank(filter) ? row : row.where(filter);
        new WriteIntoPaimonTable(
                        table,
                        DynamicOverWrite$.MODULE$,
                        TableSorter.getSorter(table, sortType, sortColumns).sort(row),
                        new Options())
                .run(spark());
        return true;
    }

    @VisibleForTesting
    static String toWhere(String partitions) {
        List<Map<String, String>> maps = ParameterUtils.getPartitions(partitions.split(";"));

        return maps.stream()
                .map(
                        a ->
                                a.entrySet().stream()
                                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                                        .reduce((s0, s1) -> s0 + " AND " + s1))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(a -> "(" + a + ")")
                .reduce((a, b) -> a + " OR " + b)
                .orElse(null);
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<CompactProcedure>() {
            @Override
            public CompactProcedure doBuild() {
                return new CompactProcedure(tableCatalog());
            }
        };
    }
}
