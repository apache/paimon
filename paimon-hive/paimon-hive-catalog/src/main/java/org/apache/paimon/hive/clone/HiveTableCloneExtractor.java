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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.FILE_COMPRESSION;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.hive.clone.HiveCloneUtils.HIDDEN_PATH_FILTER;
import static org.apache.paimon.hive.clone.HiveCloneUtils.SUPPORT_CLONE_SPLITS;
import static org.apache.paimon.hive.clone.HiveCloneUtils.parseFormat;

/** A {@link HiveCloneExtractor} for hive tables. */
public class HiveTableCloneExtractor implements HiveCloneExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(HiveTableCloneExtractor.class);
    public static final HiveTableCloneExtractor INSTANCE = new HiveTableCloneExtractor();

    @Override
    public boolean matches(Table table) {
        return true;
    }

    @Override
    public List<FieldSchema> extractSchema(
            IMetaStoreClient client, Table hiveTable, String database, String table)
            throws Exception {
        return client.getSchema(database, table);
    }

    @Override
    public List<HivePartitionFiles> extractFiles(
            Map<String, String> catalogOptions,
            IMetaStoreClient client,
            Table table,
            FileIO fileIO,
            Identifier identifier,
            RowType partitionRowType,
            String defaultPartitionName,
            @Nullable PartitionPredicate predicate)
            throws Exception {
        return listFromPureHiveTable(
                client,
                identifier,
                table,
                fileIO,
                partitionRowType,
                defaultPartitionName,
                predicate);
    }

    @Override
    public List<String> extractPartitionKeys(Table table) {
        return table.getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, String> extractOptions(Table table) {
        Map<String, String> hiveTableOptions = table.getParameters();
        Map<String, String> paimonOptions = new HashMap<>();
        String comment = hiveTableOptions.get("comment");
        if (comment != null) {
            paimonOptions.put("hive.comment", comment);
            paimonOptions.put("comment", comment);
        }

        String format = parseFormat(table);
        if (supportCloneSplits(format)) {
            Map<String, String> cloneSplitsOptions = getOptionsWhenCloneSplits(table, format);
            paimonOptions.putAll(cloneSplitsOptions);
            return paimonOptions;
        }

        paimonOptions.put(FILE_FORMAT.key(), format);
        Map<String, String> formatOptions = getIdentifierPrefixOptions(format, hiveTableOptions);
        Map<String, String> sdFormatOptions =
                getIdentifierPrefixOptions(format, table.getSd().getSerdeInfo().getParameters());
        formatOptions.putAll(sdFormatOptions);
        paimonOptions.putAll(formatOptions);

        String compression = parseCompression(table, format, formatOptions);
        if (compression != null) {
            paimonOptions.put(FILE_COMPRESSION.key(), compression);
        }
        return paimonOptions;
    }

    @Override
    public boolean supportCloneSplits(String format) {
        for (FormatTable.Format supportFormat : FormatTable.Format.values()) {
            if (supportFormat.name().equalsIgnoreCase(format)) {
                return true;
            }
        }
        return false;
    }

    private static List<HivePartitionFiles> listFromPureHiveTable(
            IMetaStoreClient client,
            Identifier identifier,
            Table sourceTable,
            FileIO fileIO,
            RowType partitionRowType,
            String defaultPartitionName,
            @javax.annotation.Nullable PartitionPredicate predicate)
            throws Exception {
        List<Partition> partitions =
                client.listPartitions(
                        identifier.getDatabaseName(), identifier.getTableName(), Short.MAX_VALUE);
        String format = parseFormat(sourceTable);

        if (partitions.isEmpty()) {
            String location = sourceTable.getSd().getLocation();
            return Collections.singletonList(
                    listFiles(fileIO, location, BinaryRow.EMPTY_ROW, format));
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
                if (predicate == null || predicate.test(partitionRow)) {
                    results.add(listFiles(fileIO, location, partitionRow, format));
                }
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

    private static String parseCompression(StorageDescriptor storageDescriptor) {
        Map<String, String> serderParams = storageDescriptor.getSerdeInfo().getParameters();
        if (serderParams.containsKey("compression")) {
            return serderParams.get("compression");
        }
        return null;
    }

    private static String parseCompression(
            Table table, String format, Map<String, String> formatOptions) {
        String compression = null;
        if (Objects.equals(format, "avro")) {
            compression = formatOptions.getOrDefault("avro.codec", parseCompression(table.getSd()));
        } else if (Objects.equals(format, "parquet")) {
            compression =
                    formatOptions.getOrDefault(
                            "parquet.compression", parseCompression(table.getSd()));
        } else if (Objects.equals(format, "orc")) {
            compression =
                    formatOptions.getOrDefault("orc.compress", parseCompression(table.getSd()));
        }
        return compression;
    }

    public static Map<String, String> getIdentifierPrefixOptions(
            String formatIdentifier, Map<String, String> options) {
        Map<String, String> result = new HashMap<>();
        String prefix = formatIdentifier.toLowerCase() + ".";
        for (String key : options.keySet()) {
            if (key.toLowerCase().startsWith(prefix)) {
                result.put(prefix + key.substring(prefix.length()), options.get(key));
            }
        }
        return result;
    }

    public static Map<String, String> getOptionsWhenCloneSplits(Table table, String format) {
        Map<String, String> result = new HashMap<>();
        if (FormatTable.Format.JSON.name().equalsIgnoreCase(format)) {
            result.put(FILE_FORMAT.key(), FormatTable.Format.PARQUET.name().toLowerCase());
        } else if (FormatTable.Format.CSV.name().equalsIgnoreCase(format)) {
            result.put(FILE_FORMAT.key(), FormatTable.Format.PARQUET.name().toLowerCase());
        } else {
            result.put(FILE_FORMAT.key(), format);
        }
        // only for clone
        result.put(SUPPORT_CLONE_SPLITS, "true");
        return result;
    }
}
