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

package org.apache.paimon.hive.migrate;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.hive.HiveTypeUtils.toPaimonType;

/** Utils for migrate Hive table to Paimon table. */
public class HiveMigrateUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMigrateUtils.class);

    private static final Predicate<FileStatus> HIDDEN_PATH_FILTER =
            p -> !p.getPath().getName().startsWith("_") && !p.getPath().getName().startsWith(".");

    public static List<Identifier> listTables(HiveCatalog hiveCatalog) throws Exception {
        IMetaStoreClient client = hiveCatalog.getHmsClient();
        List<Identifier> results = new ArrayList<>();
        for (String database : client.getAllDatabases()) {
            for (String table : client.getAllTables(database)) {
                results.add(Identifier.create(database, table));
            }
        }
        return results;
    }

    public static List<Identifier> listTables(HiveCatalog hiveCatalog, String database)
            throws Exception {
        IMetaStoreClient client = hiveCatalog.getHmsClient();
        List<Identifier> results = new ArrayList<>();
        for (String table : client.getAllTables(database)) {
            results.add(Identifier.create(database, table));
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
        List<FieldSchema> fields = client.getSchema(database, table);
        List<FieldSchema> partitionFields = hiveTable.getPartitionKeys();
        Map<String, String> hiveTableOptions = hiveTable.getParameters();

        Map<String, String> paimonOptions = new HashMap<>();
        // for compatible with hive comment system
        if (hiveTableOptions.get("comment") != null) {
            paimonOptions.put("hive.comment", hiveTableOptions.get("comment"));
        }

        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .comment(hiveTableOptions.get("comment"))
                        .options(paimonOptions)
                        .partitionKeys(
                                partitionFields.stream()
                                        .map(FieldSchema::getName)
                                        .collect(Collectors.toList()));

        fields.forEach(
                field ->
                        schemaBuilder.column(
                                field.getName(),
                                toPaimonType(field.getType()),
                                field.getComment()));

        return schemaBuilder.build();
    }

    public static List<HivePartitionFiles> listHiveFiles(
            HiveCatalog hiveCatalog,
            Identifier identifier,
            RowType partitionRowType,
            String defaultPartitionName)
            throws Exception {
        IMetaStoreClient client = hiveCatalog.getHmsClient();
        Table sourceTable =
                client.getTable(identifier.getDatabaseName(), identifier.getTableName());
        List<Partition> partitions =
                client.listPartitions(
                        identifier.getDatabaseName(), identifier.getTableName(), Short.MAX_VALUE);
        String format = parseFormat(sourceTable.getSd().getSerdeInfo().toString());

        if (partitions.isEmpty()) {
            String location = sourceTable.getSd().getLocation();
            return Collections.singletonList(
                    listFiles(hiveCatalog.fileIO(), location, BinaryRow.EMPTY_ROW, format));
        } else {
            List<BinaryWriter.ValueSetter> valueSetters = new ArrayList<>();
            partitionRowType
                    .getFieldTypes()
                    .forEach(type -> valueSetters.add(BinaryWriter.createValueSetter(type)));
            List<HivePartitionFiles> results = new ArrayList<>();
            for (Partition partition : partitions) {
                List<String> partitionValues = partition.getValues();
                String location = partition.getSd().getLocation();
                BinaryRow partitionRow =
                        FileMetaUtils.writePartitionValue(
                                partitionRowType,
                                partitionValues,
                                valueSetters,
                                defaultPartitionName);
                results.add(listFiles(hiveCatalog.fileIO(), location, partitionRow, format));
            }
            return results;
        }
    }

    private static HivePartitionFiles listFiles(
            FileIO fileIO, String location, BinaryRow partition, String format) throws IOException {
        List<FileStatus> fileStatuses =
                Arrays.stream(fileIO.listStatus(new Path(location)))
                        .filter(s -> !s.isDir())
                        .filter(HIDDEN_PATH_FILTER)
                        .collect(Collectors.toList());
        List<Path> paths = new ArrayList<>();
        List<Long> fileSizes = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            paths.add(fileStatus.getPath());
            fileSizes.add(fileStatus.getLen());
        }
        return new HivePartitionFiles(partition, paths, fileSizes, format);
    }

    private static String parseFormat(String serder) {
        if (serder.contains("avro")) {
            return "avro";
        } else if (serder.contains("parquet")) {
            return "parquet";
        } else if (serder.contains("orc")) {
            return "orc";
        } else {
            throw new UnsupportedOperationException("Unknown partition format: " + serder);
        }
    }
}
