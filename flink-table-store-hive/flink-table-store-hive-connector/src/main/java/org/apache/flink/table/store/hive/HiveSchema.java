/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.hive;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.filesystem.FileSystems;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/** Column names, types and comments of a Hive table. */
public class HiveSchema {

    private static final String TABLE_STORE_PREFIX = "tablestore.";

    private final TableSchema tableSchema;

    private HiveSchema(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    public List<String> fieldNames() {
        return tableSchema.fieldNames();
    }

    public List<LogicalType> fieldTypes() {
        return tableSchema.logicalRowType().getChildren();
    }

    public List<String> fieldComments() {
        return tableSchema.fields().stream()
                .map(DataField::description)
                .collect(Collectors.toList());
    }

    /** Extract {@link HiveSchema} from Hive serde properties. */
    public static HiveSchema extract(@Nullable Configuration configuration, Properties properties) {
        String location = properties.getProperty(hive_metastoreConstants.META_TABLE_LOCATION);
        if (location == null) {
            String tableName = properties.getProperty(hive_metastoreConstants.META_TABLE_NAME);
            throw new UnsupportedOperationException(
                    "Location property is missing for table "
                            + tableName
                            + ". Currently Flink table store only supports external table for Hive "
                            + "so location property must be set.");
        }
        Path path = new Path(location);
        if (configuration != null) {
            org.apache.flink.configuration.Configuration flinkConf =
                    org.apache.flink.configuration.Configuration.fromMap(
                            getPropsWithPrefix(configuration, TABLE_STORE_PREFIX));
            FileSystems.initialize(path, flinkConf);
        }
        TableSchema tableSchema =
                new SchemaManager(path)
                        .latest()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Schema file not found in location "
                                                        + location
                                                        + ". Please create table first."));

        if (properties.containsKey(serdeConstants.LIST_COLUMNS)
                && properties.containsKey(serdeConstants.LIST_COLUMN_TYPES)) {
            String columnNames = properties.getProperty(serdeConstants.LIST_COLUMNS);
            String columnNameDelimiter =
                    properties.getProperty(
                            // serdeConstants.COLUMN_NAME_DELIMITER is not defined in earlier Hive
                            // versions, so we use a constant string instead
                            "column.name.delimite", String.valueOf(SerDeUtils.COMMA));
            List<String> names = Arrays.asList(columnNames.split(columnNameDelimiter));

            String columnTypes = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
            List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(columnTypes);

            if (names.size() > 0 && typeInfos.size() > 0) {
                checkSchemaMatched(names, typeInfos, tableSchema);
            }
        }

        return new HiveSchema(tableSchema);
    }

    /**
     * Constructs a mapping of configuration and includes all properties that start with the
     * specified configuration prefix. Property names in the mapping are trimmed to remove the
     * configuration prefix.
     *
     * <p>Note: this is directly copied from {@link Configuration} to make E2E test happy, since
     * this method is introduced since 2.8 but we are using a hive container with hadoop-2.7.4.
     *
     * @param confPrefix configuration prefix
     * @return mapping of configuration properties with prefix stripped
     */
    private static Map<String, String> getPropsWithPrefix(Configuration conf, String confPrefix) {
        Map<String, String> configMap = new HashMap<>();
        for (Map.Entry<String, String> entry : conf) {
            String name = entry.getKey();
            if (name.startsWith(confPrefix)) {
                String value = conf.get(name);
                name = name.substring(confPrefix.length());
                configMap.put(name, value);
            }
        }
        return configMap;
    }

    private static void checkSchemaMatched(
            List<String> names, List<TypeInfo> typeInfos, TableSchema tableSchema) {
        List<String> ddlNames = new ArrayList<>(names);
        List<TypeInfo> ddlTypeInfos = new ArrayList<>(typeInfos);
        List<String> schemaNames = tableSchema.fieldNames();
        List<TypeInfo> schemaTypeInfos =
                tableSchema.logicalRowType().getChildren().stream()
                        .map(HiveTypeUtils::logicalTypeToTypeInfo)
                        .collect(Collectors.toList());

        // make the lengths of lists equal
        while (ddlNames.size() < schemaNames.size()) {
            ddlNames.add(null);
        }
        while (schemaNames.size() < ddlNames.size()) {
            schemaNames.add(null);
        }
        while (ddlTypeInfos.size() < schemaTypeInfos.size()) {
            ddlTypeInfos.add(null);
        }
        while (schemaTypeInfos.size() < ddlTypeInfos.size()) {
            schemaTypeInfos.add(null);
        }

        // compare names and type infos
        List<String> mismatched = new ArrayList<>();
        for (int i = 0; i < ddlNames.size(); i++) {
            if (!Objects.equals(ddlNames.get(i), schemaNames.get(i))
                    || !Objects.equals(ddlTypeInfos.get(i), schemaTypeInfos.get(i))) {
                String ddlField =
                        ddlNames.get(i) == null
                                ? "null"
                                : ddlNames.get(i) + " " + ddlTypeInfos.get(i).getTypeName();
                String schemaField =
                        schemaNames.get(i) == null
                                ? "null"
                                : schemaNames.get(i) + " " + schemaTypeInfos.get(i).getTypeName();
                mismatched.add(
                        String.format(
                                "Field #%d\n"
                                        + "Hive DDL          : %s\n"
                                        + "Table Store Schema: %s\n",
                                i, ddlField, schemaField));
            }
        }

        if (mismatched.size() > 0) {
            throw new IllegalArgumentException(
                    "Hive DDL and table store schema mismatched! "
                            + "It is recommended not to write any column definition "
                            + "as Flink table store external table can read schema from the specified location.\n"
                            + "Mismatched fields are:\n"
                            + String.join("--------------------\n", mismatched));
        }
    }
}
