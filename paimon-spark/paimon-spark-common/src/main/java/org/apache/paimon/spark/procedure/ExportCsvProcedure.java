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

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Export data from a Paimon table to CSV. Usage:
 *
 * <pre><code>
 *  CALL sys.export_csv(
 *      table   => 'db.tbl',
 *      path    => 'hdfs:///tmp/output',
 *      where   => 'dt = "2024-01-01"',
 *      options => map('sep', ',', 'header', 'true'))
 * </code></pre>
 *
 * <p>The output is written as a standard Spark CSV directory at the specified path. If the output
 * path already exists, it is overwritten. Nested columns (Struct/Array/Map) are serialized as JSON
 * strings; {@code quoteAll=true} is enabled by default to ensure JSON values containing commas are
 * properly quoted.
 *
 * <p>Returns a single row with {@code (result, exported_count)}.
 */
public class ExportCsvProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("path", StringType),
                ProcedureParameter.optional("where", StringType),
                ProcedureParameter.optional(
                        "options", DataTypes.createMapType(StringType, StringType)),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty()),
                        new StructField(
                                "exported_count", DataTypes.LongType, false, Metadata.empty())
                    });

    protected ExportCsvProcedure(TableCatalog tableCatalog) {
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
        String tableArg = args.getString(0);
        String path = args.getString(1);
        String whereClause = args.isNullAt(2) ? null : args.getString(2);
        Map<String, String> options =
                args.isNullAt(3) ? new HashMap<>() : toJavaMap(args.getMap(3));

        options.putIfAbsent("sep", ",");
        options.putIfAbsent("header", "true");
        options.putIfAbsent("escape", "\"");
        options.putIfAbsent("quoteAll", "true");

        Identifier ident = toIdentifier(tableArg, PARAMETERS[0].name());
        loadSparkTable(ident);

        Dataset<Row> dataset = spark().table(fullyQualifiedName(ident));

        if (whereClause != null && !whereClause.trim().isEmpty()) {
            dataset = dataset.filter(whereClause);
        }

        dataset = flattenNestedColumns(dataset);

        dataset.cache();
        long exportedCount;
        try {
            exportedCount = dataset.count();
            dataset.write().format("csv").options(options).mode("overwrite").save(path);
        } finally {
            dataset.unpersist();
        }

        return new InternalRow[] {newInternalRow(true, exportedCount)};
    }

    private static Dataset<Row> flattenNestedColumns(Dataset<Row> dataset) {
        StructType schema = dataset.schema();
        List<Column> cols = new ArrayList<>(schema.fields().length);
        boolean hasNested = false;
        for (StructField f : schema.fields()) {
            if (isNested(f.dataType())) {
                cols.add(functions.to_json(functions.col(quote(f.name()))).as(f.name()));
                hasNested = true;
            } else {
                cols.add(functions.col(quote(f.name())));
            }
        }
        return hasNested ? dataset.select(cols.toArray(new Column[0])) : dataset;
    }

    private static boolean isNested(DataType type) {
        return type instanceof StructType || type instanceof ArrayType || type instanceof MapType;
    }

    private String fullyQualifiedName(Identifier ident) {
        StringBuilder sb = new StringBuilder();
        sb.append(quote(tableCatalog().name()));
        for (String ns : ident.namespace()) {
            sb.append('.').append(quote(ns));
        }
        sb.append('.').append(quote(ident.name()));
        return sb.toString();
    }

    private static String quote(String segment) {
        return "`" + segment.replace("`", "``") + "`";
    }

    private static Map<String, String> toJavaMap(MapData mapData) {
        HashMap<String, String> map = new HashMap<>();
        if (mapData != null) {
            for (int i = 0; i < mapData.numElements(); i++) {
                String key = mapData.keyArray().getUTF8String(i).toString();
                String value =
                        mapData.valueArray().isNullAt(i)
                                ? null
                                : mapData.valueArray().getUTF8String(i).toString();
                map.put(key, value);
            }
        }
        return map;
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ExportCsvProcedure>() {
            @Override
            public ExportCsvProcedure doBuild() {
                return new ExportCsvProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "Export data from a Paimon table to a CSV directory.";
    }
}
