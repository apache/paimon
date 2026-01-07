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

package org.apache.paimon.hive;

import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FormatTable.Format;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.TableType.FORMAT_TABLE;
import static org.apache.paimon.catalog.Catalog.COMMENT_PROP;
import static org.apache.paimon.format.csv.CsvOptions.FIELD_DELIMITER;
import static org.apache.paimon.hive.HiveCatalog.HIVE_FIELD_DELIM_DEFAULT;
import static org.apache.paimon.hive.HiveCatalog.TABLE_TYPE_PROP;
import static org.apache.paimon.hive.HiveCatalog.isView;

class HiveTableUtils {

    public static Schema tryToFormatSchema(Table hiveTable) {
        if (isView(hiveTable)) {
            throw new UnsupportedOperationException("Hive view is not supported.");
        }

        Options options = Options.fromMap(hiveTable.getParameters());
        List<String> partitionKeys = getFieldNames(hiveTable.getPartitionKeys());
        RowType rowType = createRowType(hiveTable);
        String comment = options.remove(COMMENT_PROP);
        StorageDescriptor sd = hiveTable.getSd();
        if (sd == null) {
            throw new UnsupportedOperationException("Unsupported table: " + hiveTable);
        }
        String location = sd.getLocation();

        Format format;
        SerDeInfo serdeInfo = sd.getSerdeInfo();
        if (serdeInfo == null) {
            throw new UnsupportedOperationException("Unsupported table: " + hiveTable);
        }
        String serLib =
                serdeInfo.getSerializationLib() == null
                        ? ""
                        : serdeInfo.getSerializationLib().toLowerCase();
        String inputFormat = sd.getInputFormat() == null ? "" : sd.getInputFormat();
        if (serLib.contains("parquet")) {
            format = Format.PARQUET;
        } else if (serLib.contains("orc")) {
            format = Format.ORC;
        } else if (inputFormat.contains("Text")) {
            if (serLib.contains("json")) {
                format = Format.JSON;
            } else {
                if ("TEXT".equals(options.get(TABLE_TYPE_PROP))) {
                    format = Format.TEXT;
                } else {
                    format = Format.CSV;
                    options.set(
                            FIELD_DELIMITER,
                            serdeInfo
                                    .getParameters()
                                    .getOrDefault(FIELD_DELIM, HIVE_FIELD_DELIM_DEFAULT));
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported table: " + hiveTable);
        }

        Schema.Builder builder = Schema.newBuilder();
        rowType.getFields().forEach(f -> builder.column(f.name(), f.type(), f.description()));
        options.set(PATH, location);
        options.set(TYPE, FORMAT_TABLE);
        options.set(FILE_FORMAT, format.name().toLowerCase());
        return builder.partitionKeys(partitionKeys)
                .options(options.toMap())
                .comment(comment)
                .build();
    }

    /** Get field names from field schemas. */
    private static List<String> getFieldNames(List<FieldSchema> fieldSchemas) {
        List<String> names = new ArrayList<>(fieldSchemas.size());
        for (FieldSchema fs : fieldSchemas) {
            names.add(fs.getName());
        }
        return names;
    }

    /** Create a Paimon's Schema from Hive table's columns and partition keys. */
    public static RowType createRowType(Table table) {
        List<FieldSchema> allCols = new ArrayList<>(table.getSd().getCols());
        allCols.addAll(table.getPartitionKeys());

        RowType.Builder builder = RowType.builder();
        for (FieldSchema fs : allCols) {
            builder.field(fs.getName(), HiveTypeUtils.toPaimonType(fs.getType()), fs.getComment());
        }
        return builder.build();
    }
}
