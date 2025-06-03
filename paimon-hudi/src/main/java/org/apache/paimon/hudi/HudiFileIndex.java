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

package org.apache.paimon.hudi;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.hive.clone.HivePartitionFiles;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.util.StreamerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.configuration.FlinkOptions.METADATA_ENABLED;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * A file index which supports listing files efficiently through metadata table.
 *
 * @see org.apache.hudi.source.FileIndex
 */
public class HudiFileIndex {
    private static final Logger LOG = LoggerFactory.getLogger(HudiFileIndex.class);

    private final Path path;
    private final HoodieMetadataConfig metadataConfig;
    private final RowType partitionType;
    @Nullable private final PartitionPredicate partitionPredicate;
    private final HoodieEngineContext engineContext;
    private final HoodieStorage storage;
    private final HoodieTableMetaClient metaClient;
    private final List<BinaryWriter.ValueSetter> valueSetters;

    private List<String> partitionPaths;

    public HudiFileIndex(
            String location,
            Map<String, String> tableOptions,
            Map<String, String> catalogOptions,
            RowType partitionType,
            @Nullable PartitionPredicate partitionPredicate) {
        this.path = new Path(location);
        this.metadataConfig = metadataConfig(tableOptions);
        Configuration hadoopConf =
                HadoopConfigurations.getHadoopConf(
                        org.apache.flink.configuration.Configuration.fromMap(catalogOptions));
        catalogOptions.forEach(hadoopConf::set);
        this.partitionType = partitionType;
        this.partitionPredicate = partitionPredicate;
        this.engineContext = new HoodieFlinkEngineContext(hadoopConf);
        this.storage = new HoodieHadoopStorage(path, hadoopConf);
        this.metaClient = StreamerUtil.createMetaClient(location, hadoopConf);
        this.valueSetters = new ArrayList<>();
        partitionType
                .getFieldTypes()
                .forEach(type -> this.valueSetters.add(BinaryWriter.createValueSetter(type)));
    }

    public boolean isPartitioned() {
        List<String> partitionPaths = getAllPartitionPaths();
        if (partitionPaths.size() == 1 && partitionPaths.get(0).isEmpty()) {
            checkState(
                    partitionType.getFieldCount() == 0,
                    "Hudi table is non-partitioned but partition type isn't empty.");
            return false;
        } else {
            checkState(
                    partitionType.getFieldCount() >= 0,
                    "Hudi table is partitioned but partition type is empty.");
            return true;
        }
    }

    public List<HivePartitionFiles> getAllFilteredPartitionFiles(FileIO fileIO) {
        Map<String, BinaryRow> pathToRowMap = new HashMap<>();

        for (String partitionPath : getAllPartitionPaths()) {
            BinaryRow partitionRow = toPartitionRow(partitionPath);
            if (partitionPredicate == null || partitionPredicate.test(partitionRow)) {
                pathToRowMap.put(fullPartitionPath(partitionPath), partitionRow);
            }
        }

        Map<String, List<StoragePathInfo>> filePathInfos;
        try (HoodieTableMetadata tableMetadata =
                HoodieTableMetadata.create(
                        engineContext, storage, metadataConfig, path.toString())) {
            filePathInfos = tableMetadata.getAllFilesInPartitions(pathToRowMap.keySet());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<HivePartitionFiles> result = new ArrayList<>(filePathInfos.size());
        for (Map.Entry<String, List<StoragePathInfo>> filePathInfoEntry :
                filePathInfos.entrySet()) {
            result.add(
                    toPartitionFiles(
                            getBaseFiles(filePathInfoEntry.getValue()),
                            pathToRowMap.get(filePathInfoEntry.getKey()),
                            fileIO));
        }
        return result;
    }

    public HivePartitionFiles getUnpartitionedFiles(FileIO fileIO) {
        List<StoragePathInfo> filePathInfos;
        try (HoodieTableMetadata tableMetadata =
                HoodieTableMetadata.create(
                        engineContext, storage, metadataConfig, path.toString())) {
            filePathInfos = tableMetadata.getAllFilesInPartition(new StoragePath(path.toUri()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return toPartitionFiles(getBaseFiles(filePathInfos), BinaryRow.EMPTY_ROW, fileIO);
    }

    private List<HoodieBaseFile> getBaseFiles(List<StoragePathInfo> filePathInfos) {
        HoodieTableFileSystemView fsView =
                new HoodieTableFileSystemView(
                        metaClient,
                        metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants(),
                        filePathInfos);
        return fsView.getLatestBaseFiles().collect(Collectors.toList());
    }

    private BinaryRow toPartitionRow(String partitionPath) {
        List<String> partitionValues = extractPartitionValues(partitionPath);
        return FileMetaUtils.writePartitionValue(
                partitionType,
                partitionValues,
                valueSetters,
                // TODO: different engine may use different name, pass correct name later
                PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH);
    }

    private List<String> extractPartitionValues(String partitionPath) {
        String[] paths = partitionPath.split(StoragePath.SEPARATOR);
        // TODO: because we only support table in HMS, the partition is hive-style by default
        // pt1=v1/pt2=v2
        List<String> partitionValues = new ArrayList<>();
        Arrays.stream(paths)
                .forEach(
                        p -> {
                            String[] kv = p.split("=");
                            if (kv.length == 2) {
                                partitionValues.add(kv[1]);
                            } else {
                                throw new RuntimeException(
                                        "Wrong hudi partition path: " + partitionPath);
                            }
                        });
        return partitionValues;
    }

    private String fullPartitionPath(String partitionPath) {
        return new Path(path, partitionPath).toString();
    }

    private List<String> getAllPartitionPaths() {
        if (partitionPaths == null) {
            partitionPaths =
                    FSUtils.getAllPartitionPaths(
                            engineContext, storage, metadataConfig, path.toString());
        }
        return partitionPaths;
    }

    private static HoodieMetadataConfig metadataConfig(Map<String, String> conf) {
        Properties properties = new Properties();

        // set up metadata.enabled=true in table DDL to enable metadata listing
        boolean enable =
                org.apache.flink.configuration.Configuration.fromMap(conf).get(METADATA_ENABLED);
        properties.put(HoodieMetadataConfig.ENABLE.key(), enable);

        return HoodieMetadataConfig.newBuilder().fromProperties(properties).build();
    }

    private HivePartitionFiles toPartitionFiles(
            List<HoodieBaseFile> hoodieBaseFiles, BinaryRow partition, FileIO fileIO) {
        List<org.apache.paimon.fs.Path> paths = new ArrayList<>(hoodieBaseFiles.size());
        List<Long> fileSizes = new ArrayList<>(hoodieBaseFiles.size());
        String format = null;
        for (HoodieBaseFile baseFile : hoodieBaseFiles) {
            org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(baseFile.getPath());
            if (format == null) {
                format = parseFormat(path.toString());
            }
            long fileSize = baseFile.getFileSize();
            if (fileSize == -1) {
                try {
                    fileSize = fileIO.getFileSize(path);
                } catch (IOException ignored) {
                }
            }
            paths.add(path);
            fileSizes.add(fileSize);
        }
        return new HivePartitionFiles(partition, paths, fileSizes, format);
    }

    private String parseFormat(String path) {
        if (path.endsWith(".parquet")) {
            return "parquet";
        } else if (path.endsWith(".orc")) {
            return "orc";
        } else {
            throw new RuntimeException("Cannot extract format from file " + path);
        }
    }
}
