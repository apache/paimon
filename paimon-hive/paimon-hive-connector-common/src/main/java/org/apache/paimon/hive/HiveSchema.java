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

package org.apache.paimon.hive;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/** Column names, types and comments of a Hive table. */
public class HiveSchema {

    private final TableSchema tableSchema;

    private HiveSchema(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    public List<String> fieldNames() {
        return tableSchema.fieldNames();
    }

    public List<DataType> fieldTypes() {
        return tableSchema.logicalRowType().getFieldTypes();
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
                            + ". Currently Paimon only supports external table for Hive "
                            + "so location property must be set.");
        }
        Path path = new Path(location);
        Options options = PaimonJobConf.extractCatalogConfig(configuration);
        options.set(CoreOptions.PATH, path.toUri().toString());
        CatalogContext catalogContext = CatalogContext.create(options, configuration);
        TableSchema tableSchema = FileStoreTableFactory.create(catalogContext).schema();

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

    private static void checkSchemaMatched(
            List<String> names, List<TypeInfo> typeInfos, TableSchema tableSchema) {
        List<String> ddlNames = new ArrayList<>(names);
        List<TypeInfo> ddlTypeInfos = new ArrayList<>(typeInfos);
        List<String> schemaNames = tableSchema.fieldNames();
        List<TypeInfo> schemaTypeInfos =
                tableSchema.logicalRowType().getFieldTypes().stream()
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
                                "Field #%d\n" + "Hive DDL          : %s\n" + "Paimon Schema: %s\n",
                                i, ddlField, schemaField));
            }
        }

        if (mismatched.size() > 0) {
            throw new IllegalArgumentException(
                    "Hive DDL and paimon schema mismatched! "
                            + "It is recommended not to write any column definition "
                            + "as Paimon external table can read schema from the specified location.\n"
                            + "Mismatched fields are:\n"
                            + String.join("--------------------\n", mismatched));
        }
    }
}
