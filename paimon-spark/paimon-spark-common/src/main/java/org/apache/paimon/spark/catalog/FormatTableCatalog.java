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

package org.apache.paimon.spark.catalog;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.csv.CsvOptions;
import org.apache.paimon.format.text.TextOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.spark.SparkSource;
import org.apache.paimon.spark.SparkTypeUtils;
import org.apache.paimon.spark.format.PaimonFormatTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.utils.TypeUtils;

import org.apache.spark.sql.PaimonSparkSession$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.PartitionedCSVTable;
import org.apache.spark.sql.execution.PartitionedJsonTable;
import org.apache.spark.sql.execution.PartitionedOrcTable;
import org.apache.spark.sql.execution.PartitionedParquetTable;
import org.apache.spark.sql.execution.PartitionedTextTable;
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat;
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat;
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.execution.datasources.text.TextFileFormat;
import org.apache.spark.sql.execution.datasources.v2.FileTable;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Catalog supports format table. */
public interface FormatTableCatalog {

    default boolean isFormatTable(@Nullable String provide) {
        return provide != null && SparkSource.FORMAT_NAMES().contains(provide.toLowerCase());
    }

    default Table toSparkFormatTable(Identifier ident, FormatTable formatTable) {
        Map<String, String> optionsMap = formatTable.options();
        CoreOptions coreOptions = new CoreOptions(optionsMap);
        if (coreOptions.formatTableImplementationIsPaimon()) {
            return new PaimonFormatTable(formatTable);
        } else {
            SparkSession spark = PaimonSparkSession$.MODULE$.active();
            List<String> pathList = new ArrayList<>();
            pathList.add(formatTable.location());
            StructType schema = SparkTypeUtils.fromPaimonRowType(formatTable.rowType());
            StructType partitionSchema =
                    SparkTypeUtils.fromPaimonRowType(
                            TypeUtils.project(formatTable.rowType(), formatTable.partitionKeys()));
            Options options = Options.fromMap(formatTable.options());
            return convertToFileTable(
                    formatTable, ident, pathList, options, spark, schema, partitionSchema);
        }
    }

    default FileTable convertToFileTable(
            FormatTable formatTable,
            Identifier ident,
            List<String> pathList,
            Options options,
            SparkSession spark,
            StructType schema,
            StructType partitionSchema) {
        CaseInsensitiveStringMap dsOptions = new CaseInsensitiveStringMap(options.toMap());
        if (formatTable.format() == FormatTable.Format.CSV) {
            options.set("sep", options.get(CsvOptions.FIELD_DELIMITER));
            options.set("lineSep", options.get(CsvOptions.LINE_DELIMITER));
            options.set("quote", options.get(CsvOptions.QUOTE_CHARACTER));
            options.set("header", options.get(CsvOptions.INCLUDE_HEADER).toString());
            options.set("escape", options.get(CsvOptions.ESCAPE_CHARACTER));
            options.set("nullvalue", options.get(CsvOptions.NULL_LITERAL));
            options.set("mode", options.get(CsvOptions.MODE).getValue());
            if (options.contains(CoreOptions.FORMAT_TABLE_FILE_COMPRESSION)) {
                options.set("compression", options.get(CoreOptions.FORMAT_TABLE_FILE_COMPRESSION));
            }
            dsOptions = new CaseInsensitiveStringMap(options.toMap());
            return new PartitionedCSVTable(
                    ident.name(),
                    spark,
                    dsOptions,
                    scala.collection.JavaConverters.asScalaBuffer(pathList).toSeq(),
                    scala.Option.apply(schema),
                    CSVFileFormat.class,
                    partitionSchema);
        } else if (formatTable.format() == FormatTable.Format.TEXT) {
            options.set("lineSep", options.get(TextOptions.LINE_DELIMITER));
            dsOptions = new CaseInsensitiveStringMap(options.toMap());
            if (options.contains(CoreOptions.FORMAT_TABLE_FILE_COMPRESSION)) {
                options.set("compression", options.get(CoreOptions.FORMAT_TABLE_FILE_COMPRESSION));
            }
            return new PartitionedTextTable(
                    ident.name(),
                    spark,
                    dsOptions,
                    scala.collection.JavaConverters.asScalaBuffer(pathList).toSeq(),
                    scala.Option.apply(schema),
                    TextFileFormat.class,
                    partitionSchema);
        } else if (formatTable.format() == FormatTable.Format.ORC) {
            return new PartitionedOrcTable(
                    ident.name(),
                    spark,
                    dsOptions,
                    scala.collection.JavaConverters.asScalaBuffer(pathList).toSeq(),
                    scala.Option.apply(schema),
                    OrcFileFormat.class,
                    partitionSchema);
        } else if (formatTable.format() == FormatTable.Format.PARQUET) {
            return new PartitionedParquetTable(
                    ident.name(),
                    spark,
                    dsOptions,
                    scala.collection.JavaConverters.asScalaBuffer(pathList).toSeq(),
                    scala.Option.apply(schema),
                    ParquetFileFormat.class,
                    partitionSchema);
        } else if (formatTable.format() == FormatTable.Format.JSON) {
            return new PartitionedJsonTable(
                    ident.name(),
                    spark,
                    dsOptions,
                    scala.collection.JavaConverters.asScalaBuffer(pathList).toSeq(),
                    scala.Option.apply(schema),
                    JsonFileFormat.class,
                    partitionSchema);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported format table "
                            + ident.name()
                            + " format "
                            + formatTable.format().name());
        }
    }
}
