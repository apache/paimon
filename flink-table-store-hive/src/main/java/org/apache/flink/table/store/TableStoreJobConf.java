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

package org.apache.flink.table.store;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.hive.HiveTypeUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Utility class to convert Hive table property keys and get file store specific configurations from
 * {@link JobConf}.
 */
public class TableStoreJobConf {

    private static final String TBLPROPERTIES_PREFIX = "table-store.";
    private static final String TBLPROPERTIES_PRIMARY_KEYS = TBLPROPERTIES_PREFIX + "primary-keys";
    private static final String INTERNAL_TBLPROPERTIES_PREFIX =
            "table-store.internal.tblproperties.";

    private static final String INTERNAL_DB_NAME = "table-store.internal.db-name";
    private static final String INTERNAL_TABLE_NAME = "table-store.internal.table-name";
    private static final String INTERNAL_LOCATION = "table-store.internal.location";

    private static final String INTERNAL_COLUMN_NAMES = "table-store.internal.column-names";

    private static final String INTERNAL_COLUMN_TYPES = "table-store.internal.column-types";
    private static final String COLUMN_TYPES_SEPARATOR = "\0";

    private static final String INTERNAL_PARTITION_COLUMN_NAMES =
            "table-store.internal.partition-column-names";
    private static final String INTERNAL_PRIMARY_KEYS = "table-store.internal.primary-keys";

    private static final String INTERNAL_FILE_STORE_USER = "table-store.internal.file-store.user";

    private final JobConf jobConf;

    public TableStoreJobConf(JobConf jobConf) {
        this.jobConf = jobConf;
    }

    public static void update(Properties properties, Map<String, String> map) {
        String tableNameString = properties.getProperty(hive_metastoreConstants.META_TABLE_NAME);
        String[] tableName = tableNameString.split("\\.");
        Preconditions.checkState(
                tableName.length >= 2,
                "There is no dot in META_TABLE_NAME " + tableNameString + ". This is unexpected.");

        map.put(
                INTERNAL_DB_NAME,
                String.join(".", Arrays.copyOfRange(tableName, 0, tableName.length - 1)));

        map.put(INTERNAL_TABLE_NAME, tableName[tableName.length - 1]);

        map.put(
                INTERNAL_LOCATION,
                properties.getProperty(hive_metastoreConstants.META_TABLE_LOCATION));

        map.put(
                INTERNAL_COLUMN_NAMES,
                properties.getProperty(hive_metastoreConstants.META_TABLE_COLUMNS));

        List<String> serializedLogicalTypes =
                TypeInfoUtils.getTypeInfosFromTypeString(
                                properties.getProperty(
                                        hive_metastoreConstants.META_TABLE_COLUMN_TYPES))
                        .stream()
                        .map(
                                t ->
                                        HiveTypeUtils.typeInfoToDataType(t)
                                                .getLogicalType()
                                                .asSerializableString())
                        .collect(Collectors.toList());
        map.put(INTERNAL_COLUMN_TYPES, String.join(COLUMN_TYPES_SEPARATOR, serializedLogicalTypes));

        if (properties.containsKey(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS)) {
            map.put(
                    INTERNAL_PARTITION_COLUMN_NAMES,
                    properties.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS));
        }

        if (properties.containsKey(TBLPROPERTIES_PRIMARY_KEYS)) {
            map.put(INTERNAL_PRIMARY_KEYS, properties.getProperty(TBLPROPERTIES_PRIMARY_KEYS));
        }

        for (ConfigOption<?> option : FileStoreOptions.allOptions()) {
            if (properties.containsKey(TBLPROPERTIES_PREFIX + option.key())) {
                map.put(
                        INTERNAL_TBLPROPERTIES_PREFIX + option.key(),
                        properties.getProperty(TBLPROPERTIES_PREFIX + option.key()));
            }
        }
    }

    public String getDbName() {
        return jobConf.get(INTERNAL_DB_NAME);
    }

    public String getTableName() {
        return jobConf.get(INTERNAL_TABLE_NAME);
    }

    public String getLocation() {
        return jobConf.get(INTERNAL_LOCATION);
    }

    public void updateFileStoreOptions(Configuration fileStoreOptions) {
        for (Map.Entry<String, String> entry :
                jobConf.getPropsWithPrefix(INTERNAL_TBLPROPERTIES_PREFIX).entrySet()) {
            fileStoreOptions.setString(entry.getKey(), entry.getValue());
        }
    }

    public List<String> getColumnNames() {
        // see MetastoreUtils#addCols for the exact separator
        return Arrays.asList(jobConf.get(INTERNAL_COLUMN_NAMES).split(","));
    }

    public List<LogicalType> getColumnTypes() {
        return Arrays.stream(jobConf.get(INTERNAL_COLUMN_TYPES).split(COLUMN_TYPES_SEPARATOR))
                .map(LogicalTypeParser::parse)
                .collect(Collectors.toList());
    }

    public List<String> getPartitionColumnNames() {
        String partitionColumnNameString = jobConf.get(INTERNAL_PARTITION_COLUMN_NAMES);
        // see MetastoreUtils#addCols for the exact separator
        return partitionColumnNameString == null
                ? Collections.emptyList()
                : Arrays.asList(partitionColumnNameString.split("/"));
    }

    public Optional<List<String>> getPrimaryKeyNames() {
        String primaryKeyNameString = jobConf.get(INTERNAL_PRIMARY_KEYS);
        if (primaryKeyNameString == null) {
            return Optional.empty();
        } else {
            // TODO add a constant for this separator?
            return Optional.of(Arrays.asList(primaryKeyNameString.split(",")));
        }
    }

    public String getFileStoreUser() {
        return jobConf.get(INTERNAL_FILE_STORE_USER);
    }

    public void setFileStoreUser(String user) {
        jobConf.set(INTERNAL_FILE_STORE_USER, user);
    }
}
