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

import org.apache.paimon.spark.SparkTable;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Load CSV files into an existing Paimon table. Usage:
 *
 * <pre><code>
 *  CALL sys.load_csv(
 *      table   => 'db.tbl',
 *      path    => 'hdfs:///tmp/data.csv',
 *      options => map('sep', ',', 'header', 'true'))
 * </code></pre>
 *
 * <p>The reader uses the target table's schema. CSV header columns are matched to target table
 * columns by exact name. Missing columns become null, extra columns are dropped. Nested columns
 * (Struct/Array/Map) are read as STRING and restored via {@code from_json}; if the JSON is invalid,
 * the nested fields will be null but the row is still written.
 *
 * <p>The procedure always uses PERMISSIVE mode: malformed rows are counted in {@code invalid_count}
 * and excluded from the write.
 *
 * <p>Returns a single row with {@code (result, imported_count, invalid_count)}.
 */
public class LoadCsvProcedure extends BaseProcedure {

    private static final String CORRUPT_RECORD_COL_BASE = "_corrupt_record";

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("path", StringType),
                ProcedureParameter.optional(
                        "options", DataTypes.createMapType(StringType, StringType)),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty()),
                        new StructField(
                                "imported_count", DataTypes.LongType, false, Metadata.empty()),
                        new StructField(
                                "invalid_count", DataTypes.LongType, false, Metadata.empty())
                    });

    protected LoadCsvProcedure(TableCatalog tableCatalog) {
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
        Map<String, String> options =
                args.isNullAt(2) ? new HashMap<>() : toJavaMap(args.getMap(2));

        options.putIfAbsent("header", "true");
        options.putIfAbsent("sep", ",");
        options.put("mode", "PERMISSIVE");
        options.putIfAbsent("multiLine", "true");
        options.putIfAbsent("escape", "\"");

        if (!"true".equalsIgnoreCase(options.get("header"))) {
            throw new IllegalArgumentException(
                    "load_csv requires header=true; columns are matched by name.");
        }

        Identifier ident = toIdentifier(tableArg, PARAMETERS[0].name());
        SparkTable sparkTable = loadSparkTable(ident);
        StructType targetSchema = sparkTable.schema();

        CsvReadPlan plan = buildCsvReadPlan(targetSchema, options, path);
        String corruptCol = plan.corruptRecordCol;
        options.put("columnNameOfCorruptRecord", corruptCol);

        DataFrameReader reader = spark().read().options(options).schema(plan.readSchema);
        Dataset<Row> raw = reader.csv(path);

        List<Column> probeCols = new ArrayList<>();
        for (StructField f : plan.readSchema.fields()) {
            if (!corruptCol.equals(f.name())) {
                probeCols.add(functions.col(quote(f.name())));
            }
        }
        Column probeStruct = functions.struct(probeCols.toArray(new Column[0]));

        Row counts =
                raw.agg(
                                functions.count(functions.lit(1)).as("total"),
                                functions
                                        .sum(
                                                functions
                                                        .when(
                                                                functions
                                                                        .col(quote(corruptCol))
                                                                        .isNotNull(),
                                                                1L)
                                                        .otherwise(0L))
                                        .as("invalid"),
                                functions.first(probeStruct, true).as("_probe"))
                        .first();

        long total = counts.getLong(0);
        long invalidCount = counts.isNullAt(1) ? 0L : counts.getLong(1);
        long importedCount = total - invalidCount;

        Dataset<Row> notCorrupt = raw.filter(functions.col(quote(corruptCol)).isNull());
        Dataset<Row> valid = notCorrupt.select(plan.projection);

        try {
            valid.writeTo(fullyQualifiedName(ident)).append();
        } catch (NoSuchTableException e) {
            throw new RuntimeException("Failed to write to table: " + fullyQualifiedName(ident), e);
        }

        refreshSparkCache(ident, sparkTable);

        return new InternalRow[] {newInternalRow(true, importedCount, invalidCount)};
    }

    private CsvReadPlan buildCsvReadPlan(
            StructType target, Map<String, String> options, String path) {
        Set<String> targetNames = new HashSet<>();
        for (StructField f : target.fields()) {
            targetNames.add(f.name());
        }

        Map<String, String> peekOpts = new HashMap<>(options);
        peekOpts.remove("columnNameOfCorruptRecord");
        peekOpts.put("inferSchema", "false");
        String[] headerCols = spark().read().options(peekOpts).csv(path).schema().fieldNames();
        Set<String> headerSet = new HashSet<>(Arrays.asList(headerCols));

        String corruptCol = resolveCorruptRecordCol(headerSet, targetNames);

        StructType readSchema = new StructType();
        for (String h : headerCols) {
            DataType rt = StringType;
            for (StructField f : target.fields()) {
                if (f.name().equals(h)) {
                    rt = isNested(f.dataType()) ? StringType : f.dataType();
                    break;
                }
            }
            readSchema = readSchema.add(h, rt, true, Metadata.empty());
        }
        readSchema = readSchema.add(corruptCol, StringType, true, Metadata.empty());

        List<Column> cols = new ArrayList<>(target.fields().length);
        for (StructField f : target.fields()) {
            String name = f.name();
            boolean hasCol = headerSet.contains(name);
            Column src = hasCol ? functions.col(quote(name)) : functions.lit(null);
            if (isNested(f.dataType()) && hasCol) {
                cols.add(functions.from_json(src, f.dataType()).as(name));
            } else {
                cols.add(src.cast(f.dataType()).as(name));
            }
        }
        return new CsvReadPlan(readSchema, cols.toArray(new Column[0]), corruptCol);
    }

    private static String resolveCorruptRecordCol(Set<String> headerCols, Set<String> targetCols) {
        Set<String> reserved = new HashSet<>();
        for (String s : headerCols) {
            reserved.add(s.toLowerCase(Locale.ROOT));
        }
        for (String s : targetCols) {
            reserved.add(s.toLowerCase(Locale.ROOT));
        }
        String candidate = CORRUPT_RECORD_COL_BASE;
        while (reserved.contains(candidate.toLowerCase(Locale.ROOT))) {
            candidate = "_" + candidate;
        }
        return candidate;
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
        return new BaseProcedure.Builder<LoadCsvProcedure>() {
            @Override
            public LoadCsvProcedure doBuild() {
                return new LoadCsvProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "Load CSV files into an existing Paimon table.";
    }

    private static final class CsvReadPlan {
        final StructType readSchema;
        final Column[] projection;
        final String corruptRecordCol;

        CsvReadPlan(StructType readSchema, Column[] projection, String corruptRecordCol) {
            this.readSchema = readSchema;
            this.projection = projection;
            this.corruptRecordCol = corruptRecordCol;
        }
    }
}
