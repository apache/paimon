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
import org.apache.paimon.hive.utils.HiveUtils;
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
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Column names, types and comments of a Hive table. */
public class HiveSchema {

    private static final Logger LOG = LoggerFactory.getLogger(HiveSchema.class);
    private final RowType rowType;

    HiveSchema(RowType rowType) {
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
        String location = LocationKeyExtractor.getPaimonLocation(configuration, properties);
        Optional<TableSchema> tableSchema = getExistingSchema(configuration, location);
        String columnProperty = properties.getProperty(hive_metastoreConstants.META_TABLE_COLUMNS);

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
        String columnTypes =
                properties.getProperty(hive_metastoreConstants.META_TABLE_COLUMN_TYPES);
        List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(columnTypes);
        List<DataType> dataTypes =
                typeInfos.stream().map(HiveTypeUtils::toPaimonType).collect(Collectors.toList());

        // Partitions are only used for checking. They are not contained in the fields of a Hive
        // table.
        String partitionProperty =
                properties.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
        List<String> partitionKeys =
                StringUtils.isEmpty(partitionProperty)
                        ? Collections.emptyList()
                        : Arrays.asList(partitionProperty.split("/"));
        String partitionTypes =
                properties.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
        List<TypeInfo> partitionTypeInfos =
                StringUtils.isEmpty(partitionTypes)
                        ? Collections.emptyList()
                        : TypeInfoUtils.getTypeInfosFromTypeString(partitionTypes);

        String commentProperty = properties.getProperty("columns.comments");
        List<String> comments =
                StringUtils.isEmpty(commentProperty)
                        ? IntStream.range(0, columnNames.size())
                                .mapToObj(i -> "")
                                .collect(Collectors.toList())
                        : Lists.newArrayList(Splitter.on('\0').split(commentProperty));

        if (tableSchema.isPresent() && columnNames.size() > 0 && typeInfos.size() > 0) {
            // Both Paimon table schema and Hive table schema exist
            LOG.debug(
                    "Extract schema with exists DDL and exists paimon table, table location:[{}].",
                    location);

            TableSchema paimonSchema = tableSchema.get();
            String tagToPartField =
                    paimonSchema.options().get(CoreOptions.METASTORE_TAG_TO_PARTITION.key());

            boolean isPartitionedTable =
                    partitionTypeInfos.size() > 0
                            // for some Hive compatible system
                            || properties.containsKey("TABLE_TOTAL_PARTITIONS");
            checkFieldsMatched(columnNames, typeInfos, paimonSchema, isPartitionedTable);
            checkPartitionMatched(partitionKeys, partitionTypeInfos, paimonSchema, tagToPartField);

            // Use paimon table data types and column comments when the paimon table exists.
            // Using paimon data types first because hive's TypeInfoFactory.timestampTypeInfo
            // doesn't contain precision and thus may cause casting problems
            Map<String, DataField> paimonFields =
                    paimonSchema.fields().stream()
                            .collect(
                                    Collectors.toMap(
                                            dataField -> dataField.name().toLowerCase(),
                                            Function.identity()));
            for (int i = 0; i < columnNames.size(); i++) {
                String columnName = columnNames.get(i).toLowerCase();
                if (Objects.equals(columnName, tagToPartField)) {
                    // ignore tagToPartField, it should just be a string type
                    continue;
                }
                dataTypes.set(i, paimonFields.get(columnName).type());
                comments.set(i, paimonFields.get(columnName).description());
            }
        }

        RowType.Builder builder = RowType.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            builder.field(columnNames.get(i), dataTypes.get(i), comments.get(i));
        }
        return new HiveSchema(builder.build());
    }

    private static Optional<TableSchema> getExistingSchema(
            @Nullable Configuration configuration, @Nullable String location) {
        if (location == null) {
            return Optional.empty();
        }
        Path path = new Path(location);
        Options options = HiveUtils.extractCatalogConfig(configuration);
        options.set(CoreOptions.PATH, location);

        CatalogContext context = CatalogContext.create(options, configuration);
        try {
            if (!options.contains(CoreOptions.BRANCH)) {
                return new SchemaManager(FileIO.get(path, context), path).latest();
            } else {
                return new SchemaManager(
                                FileIO.get(path, context), path, options.get(CoreOptions.BRANCH))
                        .latest();
            }

        } catch (IOException e) {
            LOG.warn(
                    "Failed to fetch Paimon table schema from path "
                            + path
                            + ", relying on Hive DDL instead.",
                    e);
            return Optional.empty();
        }
    }

    private static void checkFieldsMatched(
            List<String> hiveFieldNames,
            List<TypeInfo> hiveFieldTypeInfos,
            TableSchema tableSchema,
            boolean isPartitionedTable) {
        Set<String> schemaPartitionKeySet = new HashSet<>(tableSchema.partitionKeys());
        List<String> schemaFieldNames = new ArrayList<>();
        List<TypeInfo> schemaFieldTypeInfos = new ArrayList<>();
        for (DataField field : tableSchema.fields()) {
            // case #1: if the Hive table is not a partitioned table, pick all fields
            // case #2: if the Hive table is a partitioned table, we only pick fields which are not
            //          part of partition keys
            boolean isPartitionColumn =
                    isPartitionedTable && schemaPartitionKeySet.contains(field.name());
            if (!isPartitionColumn) {
                schemaFieldNames.add(field.name());
                schemaFieldTypeInfos.add(HiveTypeUtils.toTypeInfo(field.type()));
            }
        }

        if (schemaFieldNames.size() != hiveFieldNames.size()) {
            throw new IllegalArgumentException(
                    "Hive DDL and paimon schema mismatched! "
                            + "It is recommended not to write any column definition "
                            + "as Paimon external table can read schema from the specified location.\n"
                            + "There are "
                            + hiveFieldNames.size()
                            + " fields in Hive DDL: "
                            + String.join(", ", hiveFieldNames)
                            + "\n"
                            + "There are "
                            + schemaFieldNames.size()
                            + " fields in Paimon schema: "
                            + String.join(", ", schemaFieldNames)
                            + "\n");
        }

        List<String> mismatched = new ArrayList<>();
        for (int i = 0; i < hiveFieldNames.size(); i++) {
            if (!hiveFieldNames.get(i).equalsIgnoreCase(schemaFieldNames.get(i))
                    || !Objects.equals(hiveFieldTypeInfos.get(i), schemaFieldTypeInfos.get(i))) {
                String ddlField =
                        hiveFieldNames.get(i) + " " + hiveFieldTypeInfos.get(i).getTypeName();
                String schemaField =
                        schemaFieldNames.get(i) + " " + schemaFieldTypeInfos.get(i).getTypeName();
                mismatched.add(
                        String.format(
                                "Field #%d\n" + "Hive DDL          : %s\n" + "Paimon Schema: %s\n",
                                i + 1, ddlField, schemaField));
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

    private static void checkPartitionMatched(
            List<String> hivePartitionKeys,
            List<TypeInfo> hivePartitionTypeInfos,
            TableSchema tableSchema,
            @Nullable String tagToPartField) {
        if (hivePartitionKeys.isEmpty()) {
            // only partitioned Hive table needs to consider this part
            return;
        }
        if (tagToPartField != null) {
            // fast check path for tagToPartField
            checkArgument(tableSchema.partitionKeys().isEmpty());
            checkArgument(hivePartitionKeys.equals(Collections.singletonList(tagToPartField)));
            return;
        }

        List<String> schemaPartitionKeys = tableSchema.partitionKeys();
        List<TypeInfo> schemaPartitionTypeInfos =
                tableSchema.logicalPartitionType().getFields().stream()
                        .map(f -> HiveTypeUtils.toTypeInfo(f.type()))
                        .collect(Collectors.toList());

        if (schemaPartitionKeys.size() != hivePartitionKeys.size()) {
            throw new IllegalArgumentException(
                    "Hive DDL and paimon schema mismatched! "
                            + "It is recommended not to write any column definition "
                            + "as Paimon external table can read schema from the specified location.\n"
                            + "There are "
                            + hivePartitionKeys.size()
                            + " partition keys in Hive DDL: "
                            + String.join(", ", hivePartitionKeys)
                            + "\n"
                            + "There are "
                            + schemaPartitionKeys.size()
                            + " partition keys in Paimon schema: "
                            + String.join(", ", schemaPartitionKeys)
                            + "\n");
        }

        List<String> mismatched = new ArrayList<>();
        for (int i = 0; i < hivePartitionKeys.size(); i++) {
            if (!hivePartitionKeys.get(i).equalsIgnoreCase(schemaPartitionKeys.get(i))
                    || !Objects.equals(
                            hivePartitionTypeInfos.get(i), schemaPartitionTypeInfos.get(i))) {
                String ddlField =
                        hivePartitionKeys.get(i)
                                + " "
                                + hivePartitionTypeInfos.get(i).getTypeName();
                String schemaField =
                        schemaPartitionKeys.get(i)
                                + " "
                                + schemaPartitionTypeInfos.get(i).getTypeName();
                mismatched.add(
                        String.format(
                                "Partition Key #%d\n"
                                        + "Hive DDL          : %s\n"
                                        + "Paimon Schema: %s\n",
                                i + 1, ddlField, schemaField));
            }
        }
        if (mismatched.size() > 0) {
            throw new IllegalArgumentException(
                    "Hive DDL and paimon schema mismatched! "
                            + "It is recommended not to write any column definition "
                            + "as Paimon external table can read schema from the specified location.\n"
                            + "Mismatched partition keys are:\n"
                            + String.join("--------------------\n", mismatched));
        }
    }
}
