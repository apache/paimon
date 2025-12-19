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

package org.apache.paimon.hive.clone;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.RowType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.paimon.hive.HiveTypeUtils.toPaimonType;

/** Utils for cloning Hive table to Paimon table. */
public class HiveCloneUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HiveCloneUtils.class);

    public static final Predicate<FileStatus> HIDDEN_PATH_FILTER =
            p -> !p.getPath().getName().startsWith("_") && !p.getPath().getName().startsWith(".");

    public static final String SUPPORT_CLONE_SPLITS = "support.clone.splits";

    public static Map<String, String> getDatabaseOptions(
            HiveCatalog hiveCatalog, String databaseName) throws Exception {
        IMetaStoreClient client = hiveCatalog.getHmsClient();
        Database database = client.getDatabase(databaseName);
        Map<String, String> paimonOptions = new HashMap<>();
        if (database.getDescription() != null) {
            paimonOptions.put("comment", database.getDescription());
        }
        return paimonOptions;
    }

    public static List<Identifier> listTables(
            HiveCatalog hiveCatalog,
            @Nullable List<String> includedTables,
            @Nullable List<String> excludedTables)
            throws Exception {
        Set<String> includedTableSet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(includedTables)) {
            includedTableSet.addAll(includedTables);
        }
        Set<String> excludedTableSet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(excludedTables)) {
            excludedTableSet.addAll(excludedTables);
        }
        IMetaStoreClient client = hiveCatalog.getHmsClient();
        List<Identifier> results = new ArrayList<>();
        for (String database : client.getAllDatabases()) {
            for (String table : client.getAllTables(database)) {
                Identifier identifier = Identifier.create(database, table);
                if (excludedTableSet.contains(identifier.getFullName())) {
                    continue;
                }
                if (CollectionUtils.isEmpty(includedTableSet)
                        || includedTableSet.contains(identifier.getFullName())) {
                    results.add(identifier);
                }
            }
        }
        return results;
    }

    public static List<Identifier> listTables(
            HiveCatalog hiveCatalog,
            String database,
            @Nullable List<String> includedTables,
            @Nullable List<String> excludedTables)
            throws Exception {
        Set<String> includedTableSet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(includedTables)) {
            includedTableSet.addAll(includedTables);
        }
        Set<String> excludedTableSet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(excludedTables)) {
            excludedTableSet.addAll(excludedTables);
        }
        IMetaStoreClient client = hiveCatalog.getHmsClient();
        List<Identifier> results = new ArrayList<>();
        for (String table : client.getAllTables(database)) {
            Identifier identifier = Identifier.create(database, table);
            if (excludedTableSet.contains(identifier.getFullName())) {
                continue;
            }
            if (CollectionUtils.isEmpty(includedTableSet)
                    || includedTableSet.contains(identifier.getFullName())) {
                results.add(identifier);
            }
        }
        return results;
    }

    public static Schema hiveTableToPaimonSchema(HiveCatalog hiveCatalog, Identifier identifier)
            throws Exception {
        String database = identifier.getDatabaseName();
        String table = identifier.getObjectName();

        IMetaStoreClient client = hiveCatalog.getHmsClient();
        // check primary key
        PrimaryKeysRequest primaryKeysRequest = new PrimaryKeysRequest(database, table);
        try {
            if (!client.getPrimaryKeys(primaryKeysRequest).isEmpty()) {
                throw new IllegalArgumentException("Can't migrate primary key table yet.");
            }
        } catch (Exception e) {
            LOG.warn(
                    "Your Hive version is low which not support get_primary_keys, skip primary key check firstly!");
        }

        Table hiveTable = client.getTable(database, table);
        HiveCloneExtractor extractor = HiveCloneExtractor.getExtractor(hiveTable);
        List<FieldSchema> fields = extractor.extractSchema(client, hiveTable, database, table);
        List<String> partitionKeys = extractor.extractPartitionKeys(hiveTable);
        Map<String, String> options = extractor.extractOptions(hiveTable);
        if (hiveTable.isSetPartitionKeys()) {
            options.put(
                    PARTITION_DEFAULT_NAME.key(),
                    client.getConfigValue(
                            "hive.exec.default.partition.name", "__HIVE_DEFAULT_PARTITION__"));
        }

        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .comment(options.get("comment"))
                        .options(options)
                        .partitionKeys(partitionKeys);

        fields.forEach(
                field ->
                        schemaBuilder.column(
                                field.getName(),
                                toPaimonType(field.getType()),
                                field.getComment()));

        return schemaBuilder.build();
    }

    public static List<HivePartitionFiles> listFiles(
            HiveCatalog hiveCatalog,
            Identifier identifier,
            RowType partitionRowType,
            String defaultPartitionName,
            @Nullable PartitionPredicate predicate)
            throws Exception {
        IMetaStoreClient client = hiveCatalog.getHmsClient();
        Table sourceTable =
                client.getTable(identifier.getDatabaseName(), identifier.getTableName());
        return HiveCloneExtractor.getExtractor(sourceTable)
                .extractFiles(
                        hiveCatalog.options(),
                        hiveCatalog.getHmsClient(),
                        sourceTable,
                        hiveCatalog.fileIO(),
                        identifier,
                        partitionRowType,
                        defaultPartitionName,
                        predicate);
    }

    private static String parseFormat(StorageDescriptor sd) {
        SerDeInfo serdeInfo = sd.getSerdeInfo();
        if (serdeInfo == null) {
            return null;
        }
        String serLib =
                serdeInfo.getSerializationLib() == null
                        ? ""
                        : serdeInfo.getSerializationLib().toLowerCase();
        String inputFormat = sd.getInputFormat() == null ? "" : sd.getInputFormat();
        if (serLib.contains("avro")) {
            return "avro";
        } else if (serLib.contains("parquet")) {
            return "parquet";
        } else if (serLib.contains("orc")) {
            return "orc";
        } else if (inputFormat.contains("Text")) {
            if (serLib.contains("json")) {
                return "json";
            } else {
                return "csv";
            }
        }
        return null;
    }

    public static String parseFormat(Table table) {
        String format = parseFormat(table.getSd());
        if (format == null) {
            throw new UnsupportedOperationException("Unknown table format:" + table);
        }
        return format;
    }

    public static String parseFormat(Partition partition) {
        String format = parseFormat(partition.getSd());
        if (format == null) {
            throw new UnsupportedOperationException("Unknown partition format: " + partition);
        }
        return format;
    }
}
