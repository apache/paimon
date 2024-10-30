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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.FormatTable.Format;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.TableType.FORMAT_TABLE;
import static org.apache.paimon.catalog.Catalog.COMMENT_PROP;
import static org.apache.paimon.table.FormatTableOptions.FIELD_DELIMITER;

class HiveTableUtils {

    public static FormatTable convertToFormatTable(Table hiveTable) {
        if (TableType.valueOf(hiveTable.getTableType()) == TableType.VIRTUAL_VIEW) {
            throw new UnsupportedOperationException("Hive view is not supported.");
        }

        Identifier identifier = new Identifier(hiveTable.getDbName(), hiveTable.getTableName());
        Map<String, String> options = new HashMap<>(hiveTable.getParameters());
        List<String> partitionKeys = getFieldNames(hiveTable.getPartitionKeys());
        RowType rowType = createRowType(hiveTable);
        String comment = options.remove(COMMENT_PROP);
        String location = hiveTable.getSd().getLocation();
        Format format;
        SerDeInfo serdeInfo = hiveTable.getSd().getSerdeInfo();
        if (Options.fromMap(options).get(TYPE) == FORMAT_TABLE) {
            format = Format.valueOf(options.get(FILE_FORMAT.key()).toUpperCase());
            if (format.equals(Format.CSV)) {
                options.put(
                        FIELD_DELIMITER.key(),
                        serdeInfo.getParameters().getOrDefault(FIELD_DELIM, "\u0001"));
            }
        } else {
            String serLib = serdeInfo.getSerializationLib().toLowerCase();
            String inputFormat = hiveTable.getSd().getInputFormat();
            if (serLib.contains("parquet")) {
                format = Format.PARQUET;
            } else if (serLib.contains("orc")) {
                format = Format.ORC;
            } else if (inputFormat.contains("Text")) {
                format = Format.CSV;
                // hive default field delimiter is '\u0001'
                options.put(
                        FIELD_DELIMITER.key(),
                        serdeInfo.getParameters().getOrDefault(FIELD_DELIM, "\u0001"));
            } else {
                throw new UnsupportedOperationException("Unsupported table: " + hiveTable);
            }
        }
        return FormatTable.builder()
                .identifier(identifier)
                .rowType(rowType)
                .partitionKeys(partitionKeys)
                .location(location)
                .format(format)
                .options(options)
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
        Pair<String[], DataType[]> columnInformation = extractColumnInformation(allCols);
        return RowType.builder()
                .fields(columnInformation.getRight(), columnInformation.getLeft())
                .build();
    }

    private static Pair<String[], DataType[]> extractColumnInformation(List<FieldSchema> allCols) {
        String[] colNames = new String[allCols.size()];
        DataType[] colTypes = new DataType[allCols.size()];

        for (int i = 0; i < allCols.size(); i++) {
            FieldSchema fs = allCols.get(i);
            colNames[i] = fs.getName();
            colTypes[i] =
                    HiveTypeUtils.toPaimonType(
                            TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
        }

        return Pair.of(colNames, colTypes);
    }
}
