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
import org.apache.paimon.table.TableUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.base.Splitter;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.paimon.hive.HiveTypeUtils.typeInfoToLogicalType;

/** Column names, types and comments of a Hive table. */
public class HiveSchema {

    private static final Logger LOG = LoggerFactory.getLogger(HiveSchema.class);

    private final TableSchema tableSchema;
    private final RowType rowType;

    private HiveSchema(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
        this.rowType = new RowType(tableSchema.fields());
    }

    public RowType rowType() {
        return rowType;
    }

    public List<String> fieldNames() {
        return tableSchema.fieldNames();
    }

    public List<DataType> fieldTypes() {
        return tableSchema.logicalRowType().getFieldTypes();
    }

    public List<DataField> fields() {
        return tableSchema.fields();
    }

    public List<String> fieldComments() {
        return schemaComments(tableSchema);
    }

    /** Extract {@link HiveSchema} from Hive serde properties. */
    public static HiveSchema extract(@Nullable Configuration configuration, Properties properties) {
        String location = properties.getProperty(hive_metastoreConstants.META_TABLE_LOCATION);
        if (location == null) {
            String tableName = properties.getProperty(hive_metastoreConstants.META_TABLE_NAME);
            throw new UnsupportedOperationException(
                    "Location property is missing for table "
                            + tableName
                            + ". Currently Paimon only supports hive table location property must be set.");
        }
        Path path = new Path(location);
        Options options = PaimonJobConf.extractCatalogConfig(configuration);
        options.set(CoreOptions.PATH, path.toUri().toString());
        CatalogContext catalogContext = CatalogContext.create(options, configuration);
        Optional<TableSchema> tableSchemaOptional = TableUtils.schema(catalogContext);

        String columnProperty = properties.getProperty(serdeConstants.LIST_COLUMNS);
        // Create hive external table with empty ddl
        if (StringUtils.isEmpty(columnProperty)) {
            if (!tableSchemaOptional.isPresent()) {
                throw new IllegalArgumentException(
                        "Schema file not found in location "
                                + location
                                + ". Please create table first.");
            }
            // Paimon external table can read schema from the specified location
            return new HiveSchema(tableSchemaOptional.get());
        }

        // Create hive external table with ddl
        String columnNameDelimiter =
                properties.getProperty(
                        // serdeConstants.COLUMN_NAME_DELIMITER is not defined in earlier Hive
                        // versions, so we use a constant string instead
                        "column.name.delimite", String.valueOf(SerDeUtils.COMMA));
        List<String> columnNames = Arrays.asList(columnProperty.split(columnNameDelimiter));
        String columnTypes = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(columnTypes);
        List<String> comments =
                Lists.newArrayList(
                        Splitter.on('\0').split(properties.getProperty("columns.comments")));
        // Both Paimon table schema and Hive table schema exist
        if (tableSchemaOptional.isPresent() && columnNames.size() > 0 && typeInfos.size() > 0) {
            LOG.debug(
                    "Extract schema with exists DDL and exists paimon table, table location:[{}].",
                    location);
            checkSchemaMatched(columnNames, typeInfos, tableSchemaOptional.get());
            comments = schemaComments(tableSchemaOptional.get());
        }

        int highestFieldId = -1;
        List<DataField> columns = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.add(
                    new DataField(
                            ++highestFieldId,
                            columnNames.get(i),
                            typeInfoToLogicalType(typeInfos.get(i)),
                            comments.get(i)));
        }
        TableSchema tableSchema =
                new TableSchema(
                        0L,
                        columns,
                        highestFieldId,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options.toMap(),
                        properties.getProperty("comment"));
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

    private static List<String> schemaComments(TableSchema tableSchema) {
        return tableSchema.fields().stream()
                .map(DataField::description)
                .collect(Collectors.toList());
    }
}
