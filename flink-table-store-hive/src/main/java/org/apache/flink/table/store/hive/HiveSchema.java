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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

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

    private static final String TBLPROPERTIES_PREFIX = "table-store.";
    private static final String TBLPROPERTIES_PRIMARY_KEYS = TBLPROPERTIES_PREFIX + "primary-keys";

    private final Schema schema;
    private final List<String> fieldComments;
    private final Map<String, String> tableStoreOptions;

    private HiveSchema(
            Schema schema, List<String> fieldComments, Map<String, String> tableStoreOptions) {
        Preconditions.checkArgument(
                schema.fields().size() == fieldComments.size(),
                "Length of schema fields (%s) (%s) and comments (%s) are different.",
                schema.fields().size(),
                fieldComments.size());
        this.schema = schema;
        this.fieldComments = fieldComments;
        this.tableStoreOptions = tableStoreOptions;
    }

    public List<String> fieldNames() {
        return schema.fieldNames();
    }

    public List<LogicalType> fieldTypes() {
        return schema.logicalRowType().getChildren();
    }

    public List<String> fieldComments() {
        return fieldComments;
    }

    public Map<String, String> tableStoreOptions() {
        return tableStoreOptions;
    }

    /** Extract {@link HiveSchema} from Hive serde properties. */
    public static HiveSchema extract(Properties properties) {
        String columnNames = properties.getProperty(serdeConstants.LIST_COLUMNS);
        String columnNameDelimiter =
                properties.getProperty(
                        serdeConstants.COLUMN_NAME_DELIMITER, String.valueOf(SerDeUtils.COMMA));
        List<String> names = Arrays.asList(columnNames.split(columnNameDelimiter));

        String columnTypes = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(columnTypes);

        List<String> partitionKeys = new ArrayList<>();
        if (properties.containsKey(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS)) {
            String partitionKeysString =
                    properties.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
            // see MetastoreUtils#addCols for the exact separator
            partitionKeys = Arrays.asList(partitionKeysString.split("/"));
        }

        List<String> primaryKeys = new ArrayList<>();
        if (properties.containsKey(TBLPROPERTIES_PRIMARY_KEYS)) {
            String primaryKeysString = properties.getProperty(TBLPROPERTIES_PRIMARY_KEYS);
            // TODO add a constant for this separator?
            primaryKeys =
                    Arrays.stream(primaryKeysString.split(","))
                            .map(String::trim)
                            .collect(Collectors.toList());
        }

        String location = properties.getProperty(hive_metastoreConstants.META_TABLE_LOCATION);
        if (location == null) {
            String tableName = properties.getProperty(hive_metastoreConstants.META_TABLE_NAME);
            throw new UnsupportedOperationException(
                    "Location property is missing for table "
                            + tableName
                            + ". Currently Flink table store only supports external table for Hive "
                            + "so location property must be set.");
        }
        Schema schema =
                new SchemaManager(new Path(location))
                        .latest()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Schema file not found in location "
                                                        + location
                                                        + ". Please create table first."));
        checkSchemaMatched(names, typeInfos, partitionKeys, primaryKeys, schema);

        // see MetastoreUtils#addCols for the exact property name and separator
        String columnCommentsPropertyName = "columns.comments";
        List<String> comments =
                Arrays.asList(properties.getProperty(columnCommentsPropertyName).split("\0", -1));

        Map<String, String> tableStoreOptions = new HashMap<>();
        for (ConfigOption<?> option : FileStoreOptions.allOptions()) {
            if (properties.containsKey(TBLPROPERTIES_PREFIX + option.key())) {
                tableStoreOptions.put(
                        option.key(), properties.getProperty(TBLPROPERTIES_PREFIX + option.key()));
            }
        }

        return new HiveSchema(schema, comments, tableStoreOptions);
    }

    private static void checkSchemaMatched(
            List<String> names,
            List<TypeInfo> typeInfos,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Schema schema) {
        List<String> ddlNames = new ArrayList<>(names);
        List<TypeInfo> ddlTypeInfos = new ArrayList<>(typeInfos);
        List<String> schemaNames = schema.fieldNames();
        List<TypeInfo> schemaTypeInfos =
                schema.logicalRowType().getChildren().stream()
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
                    "Hive DDL and table store schema mismatched! Mismatched fields are:\n"
                            + String.join("--------------------\n", mismatched));
        }

        if (!Objects.equals(partitionKeys, schema.partitionKeys())) {
            throw new IllegalArgumentException(
                    "Hive DDL and table store schema have different partition keys!\n"
                            + "Hive DDL          : "
                            + partitionKeys.toString()
                            + "\n"
                            + "Table Store Schema: "
                            + schema.partitionKeys().toString());
        }

        if (!Objects.equals(primaryKeys, schema.primaryKeys())) {
            throw new IllegalArgumentException(
                    "Hive DDL and table store schema have different primary keys!\n"
                            + "Hive DDL          : "
                            + primaryKeys.toString()
                            + "\n"
                            + "Table Store Schema: "
                            + schema.primaryKeys().toString());
        }
    }
}
