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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.base.Splitter;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.paimon.hive.HiveTypeUtils.typeInfoToLogicalType;

/** Column names, types and comments of a Hive table. */
public class HiveSchema {

    private static final Logger LOG = LoggerFactory.getLogger(HiveSchema.class);
    private final RowType rowType;

    private HiveSchema(RowType rowType) {
        this.rowType = rowType;
    }

    public RowType rowType() {
        return rowType;
    }

    public List<String> fieldNames() {
        return rowType.getFieldNames();
    }

    public List<DataType> fieldTypes() {
        return rowType.getFieldTypes();
    }

    public List<DataField> fields() {
        return rowType.getFields();
    }

    public List<String> fieldComments() {
        return rowType.getFields().stream()
                .map(DataField::description)
                .collect(Collectors.toList());
    }

    /** Extract {@link HiveSchema} from Hive serde properties. */
    public static HiveSchema extract(@Nullable Configuration configuration, Properties properties) {
        String location = LocationKeyExtractor.getLocation(properties);
        Optional<TableSchema> tableSchema = getExistsSchema(configuration, location);
        String columnProperty = properties.getProperty(serdeConstants.LIST_COLUMNS);
        // Create hive external table with empty ddl
        if (StringUtils.isEmpty(columnProperty)) {
            if (!tableSchema.isPresent()) {
                throw new IllegalArgumentException(
                        "Schema file not found in location "
                                + location
                                + ". Please create table first.");
            }
            // Paimon external table can read schema from the specified location
            return new HiveSchema(new RowType(tableSchema.get().fields()));
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
        if (tableSchema.isPresent() && columnNames.size() > 0 && typeInfos.size() > 0) {
            LOG.debug(
                    "Extract schema with exists DDL and exists paimon table, table location:[{}].",
                    location);
            checkSchemaMatched(columnNames, typeInfos, tableSchema.get());
            // Use paimon table column comment when the paimon table exists.
            comments =
                    tableSchema.get().fields().stream()
                            .map(DataField::description)
                            .collect(Collectors.toList());
        }
        RowType.Builder builder = RowType.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            builder.field(
                    columnNames.get(i), typeInfoToLogicalType(typeInfos.get(i)), comments.get(i));
        }
        return new HiveSchema(builder.build());
    }

    private static Optional<TableSchema> getExistsSchema(
            @Nullable Configuration configuration, @Nullable String location) {
        if (location == null) {
            return Optional.empty();
        }
        Path path = new Path(location);
        Options options = PaimonJobConf.extractCatalogConfig(configuration);
        options.set(CoreOptions.PATH, location);
        CatalogContext context = CatalogContext.create(options, configuration);
        try {
            return new SchemaManager(FileIO.get(path, context), path).latest();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
