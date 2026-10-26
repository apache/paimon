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

import org.apache.paimon.annotation.VisibleForTesting;
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
import java.util.Locale;
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

    /** Formats whose data files can be copied into a Paimon table as they are. */
    private static final List<String> COPY_FILES_FORMATS = Arrays.asList("orc", "parquet", "avro");

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
        return extractFiles(
                catalogOptions,
                client,
                table,
                fileIO,
                identifier,
                partitionRowType,
                defaultPartitionName,
                predicate,
                false);
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
            @Nullable PartitionPredicate predicate,
            boolean recursive)
            throws Exception {
        return listFromPureHiveTable(
                client,
                identifier,
                table,
                fileIO,
                partitionRowType,
                defaultPartitionName,
                predicate,
                recursive);
    }

    @Override
    public List<String> extractPartitionKeys(Table table) {
        return table.getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, String> extractOptions(Table table) {
        return extractOptions(table, false);
    }

    @Override
    public Map<String, String> extractOptions(Table table, boolean copyFiles) {
        Map<String, String> hiveTableOptions = table.getParameters();
        if (copyFiles && "true".equalsIgnoreCase(hiveTableOptions.get("transactional"))) {
            // ACID files wrap rows with transaction metadata, and aborted writes leave files behind
            throw new UnsupportedOperationException(
                    "Copying data files is not supported for transactional tables.");
        }
        Map<String, String> paimonOptions = new HashMap<>();
        String comment = hiveTableOptions.get("comment");
        if (comment != null) {
            paimonOptions.put("hive.comment", comment);
            paimonOptions.put("comment", comment);
        }

        String format = parseFormat(table);
        if (copyFiles && !COPY_FILES_FORMATS.contains(format)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Copying data files is only supported for formats %s, but table has format '%s'.",
                            COPY_FILES_FORMATS, format));
        }
        if (!copyFiles && supportCloneSplits(format)) {
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
            @javax.annotation.Nullable PartitionPredicate predicate,
            boolean recursive)
            throws Exception {
        int[] partitionValueIndexes =
                partitionValueIndexes(
                        identifier,
                        INSTANCE.extractPartitionKeys(sourceTable),
                        partitionRowType.getFieldNames());
        List<Partition> partitions =
                client.listPartitions(
                        identifier.getDatabaseName(), identifier.getTableName(), (short) -1);
        String format = parseFormat(sourceTable);

        if (partitions.isEmpty()) {
            if (partitionRowType.getFieldCount() > 0) {
                // files under the table location belong to partitions unknown to the metastore
                LOG.warn(
                        "Partitioned table {} has no partitions in the metastore, no files are cloned.",
                        identifier);
                return Collections.emptyList();
            }
            String location = sourceTable.getSd().getLocation();
            return Collections.singletonList(
                    listFiles(fileIO, location, BinaryRow.EMPTY_ROW, format, recursive));
        } else {
            List<BinaryWriter.ValueSetter> valueSetters = new ArrayList<>();
            partitionRowType
                    .getFieldTypes()
                    .forEach(type -> valueSetters.add(BinaryWriter.createValueSetter(type)));
            List<HivePartitionFiles> results = new ArrayList<>();
            for (Partition partition : partitions) {
                List<String> partitionValues = new ArrayList<>();
                for (int index : partitionValueIndexes) {
                    partitionValues.add(partition.getValues().get(index));
                }
                String location = partition.getSd().getLocation();
                BinaryRow partitionRow =
                        FileMetaUtils.writePartitionValue(
                                partitionRowType,
                                partitionValues,
                                valueSetters,
                                defaultPartitionName);
                if (predicate == null || predicate.test(partitionRow)) {
                    checkPartitionColumns(identifier, sourceTable, partition);
                    results.add(listFiles(fileIO, location, partitionRow, format, recursive));
                }
            }
            if (recursive) {
                checkNoSharedFiles(identifier, results);
            }
            return results;
        }
    }

    /**
     * Hive partition values follow the order of the source partition keys, and the partition row
     * follows the order of the target partition keys. Returns, for each target key, the index of
     * its value in the Hive partition values.
     */
    @VisibleForTesting
    static int[] partitionValueIndexes(
            Identifier identifier, List<String> sourceKeys, List<String> targetKeys) {
        int[] indexes = new int[targetKeys.size()];
        boolean match = sourceKeys.size() == targetKeys.size();
        for (int i = 0; match && i < targetKeys.size(); i++) {
            indexes[i] = sourceKeys.indexOf(targetKeys.get(i));
            match = indexes[i] >= 0;
        }
        if (!match) {
            throw new IllegalStateException(
                    String.format(
                            "The partition keys %s of table %s do not match the partition keys %s of the target table.",
                            sourceKeys, identifier, targetKeys));
        }
        return indexes;
    }

    /**
     * Copied files are read with the table columns, so a partition must not declare a column of the
     * table with another type. Columns missing from a partition are read as null.
     */
    private static void checkPartitionColumns(
            Identifier identifier, Table table, Partition partition) {
        if (!partition.getSd().isSetCols()) {
            return;
        }
        Map<String, String> tableTypes = new HashMap<>();
        for (FieldSchema column : table.getSd().getCols()) {
            tableTypes.put(column.getName(), column.getType());
        }
        for (FieldSchema column : partition.getSd().getCols()) {
            String tableType = tableTypes.get(column.getName());
            if (tableType != null && !tableType.equalsIgnoreCase(column.getType())) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Can not copy the files of partition %s of table %s: column '%s' has type '%s' in the partition but '%s' in the table.",
                                partition.getValues(),
                                identifier,
                                column.getName(),
                                column.getType(),
                                tableType));
            }
        }
    }

    /**
     * Listing recursively, a partition located inside another partition would have its files cloned
     * twice.
     */
    @VisibleForTesting
    static void checkNoSharedFiles(Identifier identifier, List<HivePartitionFiles> partitions) {
        Map<Path, BinaryRow> owners = new HashMap<>();
        for (HivePartitionFiles partitionFiles : partitions) {
            for (Path path : partitionFiles.paths()) {
                BinaryRow owner = owners.putIfAbsent(path, partitionFiles.partition());
                if (owner != null) {
                    throw new IllegalStateException(
                            String.format(
                                    "File %s of table %s is listed by more than one partition, "
                                            + "check the partition locations or disable recursive listing.",
                                    path, identifier));
                }
            }
        }
    }

    @VisibleForTesting
    static HivePartitionFiles listFiles(
            FileIO fileIO, String location, BinaryRow partition, String format, boolean recursive)
            throws IOException {
        Path root = new Path(location);
        List<FileStatus> fileStatuses;
        if (recursive && fileIO.isObjectStore()) {
            // one flat listing of the prefix, listing each directory would need many more requests
            fileStatuses =
                    Arrays.stream(fileIO.listFiles(root, true))
                            .filter(HIDDEN_PATH_FILTER)
                            .filter(s -> !isInHiddenDirectory(root, s.getPath()))
                            .collect(Collectors.toList());
        } else if (recursive) {
            // hidden directories may not be readable, so they are skipped before being listed
            fileStatuses = new ArrayList<>();
            listVisibleFiles(fileIO, root, fileStatuses);
        } else {
            fileStatuses =
                    Arrays.stream(fileIO.listStatus(root))
                            .filter(s -> !s.isDir())
                            .filter(HIDDEN_PATH_FILTER)
                            .collect(Collectors.toList());
        }
        List<Path> paths = new ArrayList<>();
        List<Long> fileSizes = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            paths.add(fileStatus.getPath());
            fileSizes.add(fileStatus.getLen());
        }
        return new HivePartitionFiles(partition, paths, fileSizes, format);
    }

    private static void listVisibleFiles(FileIO fileIO, Path dir, List<FileStatus> files)
            throws IOException {
        for (FileStatus status : fileIO.listStatus(dir)) {
            if (!HIDDEN_PATH_FILTER.test(status)) {
                continue;
            }
            if (status.isDir()) {
                listVisibleFiles(fileIO, status.getPath(), files);
            } else {
                files.add(status);
            }
        }
    }

    /**
     * Whether any directory between the root and the file is hidden, like the '_temporary' and
     * '.hive-staging' directories left by failed jobs. The same convention is used by Hadoop's
     * input formats.
     */
    private static boolean isInHiddenDirectory(Path root, Path file) {
        String rootPath = trimTrailingSlash(root.toUri().getPath());
        Path directory = file.getParent();
        while (directory != null) {
            String directoryPath = trimTrailingSlash(directory.toUri().getPath());
            if (directoryPath.length() <= rootPath.length()) {
                return false;
            }
            String name = directory.getName();
            if (name.startsWith("_") || name.startsWith(".")) {
                return true;
            }
            directory = directory.getParent();
        }
        return false;
    }

    private static String trimTrailingSlash(String path) {
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
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
        // match against the identifier as written so the suffix is sliced at an offset the key
        // actually has: lower-casing can lengthen a string, and U+0130 lower-cases to two chars
        String prefix = formatIdentifier + ".";
        String lowerCasePrefix = formatIdentifier.toLowerCase(Locale.ROOT) + ".";
        for (String key : options.keySet()) {
            if (key.regionMatches(true, 0, prefix, 0, prefix.length())) {
                result.put(lowerCasePrefix + key.substring(prefix.length()), options.get(key));
            }
        }
        return result;
    }

    public static Map<String, String> getOptionsWhenCloneSplits(Table table, String format) {
        Map<String, String> result = new HashMap<>();
        if (FormatTable.Format.JSON.name().equalsIgnoreCase(format)) {
            result.put(
                    FILE_FORMAT.key(), FormatTable.Format.PARQUET.name().toLowerCase(Locale.ROOT));
        } else if (FormatTable.Format.CSV.name().equalsIgnoreCase(format)) {
            result.put(
                    FILE_FORMAT.key(), FormatTable.Format.PARQUET.name().toLowerCase(Locale.ROOT));
        } else {
            result.put(FILE_FORMAT.key(), format);
        }
        // only for clone
        result.put(SUPPORT_CLONE_SPLITS, "true");
        return result;
    }
}
