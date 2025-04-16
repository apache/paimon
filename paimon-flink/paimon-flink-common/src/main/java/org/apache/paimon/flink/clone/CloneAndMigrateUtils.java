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

package org.apache.paimon.flink.clone;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.migrate.HiveMigrateUtils;
import org.apache.paimon.hive.migrate.HivePartitionFiles;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StatsCollectorFactories;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

public class CloneAndMigrateUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CloneAndMigrateUtils.class);

    public static DataStream<Tuple2<Identifier, Identifier>> buildSource(
            String sourceDatabase,
            String sourceTableName,
            String targetDatabase,
            String targetTableName,
            HiveCatalog sourceCatalog,
            StreamExecutionEnvironment env)
            throws Exception {
        List<Tuple2<Identifier, Identifier>> result = new ArrayList<>();

        if (StringUtils.isNullOrWhitespaceOnly(sourceDatabase)) {
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(sourceTableName),
                    "sourceTableName must be blank when database is null.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must be blank when clone all tables in a catalog.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must be blank when clone all tables in a catalog.");

            for (Identifier identifier : HiveMigrateUtils.listTables(sourceCatalog)) {
                result.add(new Tuple2<>(identifier, identifier));
            }
        } else if (StringUtils.isNullOrWhitespaceOnly(sourceTableName)) {
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must not be blank when clone all tables in a database.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must be blank when clone all tables in a catalog.");

            for (Identifier identifier :
                    HiveMigrateUtils.listTables(sourceCatalog, sourceDatabase)) {
                result.add(
                        new Tuple2<>(
                                identifier,
                                Identifier.create(targetDatabase, identifier.getObjectName())));
            }
        } else {
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must not be blank when clone a table.");
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must not be blank when clone a table.");
            result.add(
                    new Tuple2<>(
                            Identifier.create(sourceDatabase, sourceTableName),
                            Identifier.create(targetDatabase, targetTableName)));
        }

        checkState(!result.isEmpty(), "Didn't find any table in source catalog.");

        if (LOG.isDebugEnabled()) {
            LOG.debug("The clone identifiers of source table and target table are: {}", result);
        }
        return env.fromCollection(result).forceNonParallel();
    }

    public static ProcessFunction<Tuple2<Identifier, Identifier>, List<MigrateFilesInfo>>
            CreateTargetTableAndListFilesFunction(
                    Map<String, String> sourceCatalogConfig,
                    Map<String, String> targetCatalogConfig) {
        return new ProcessFunction<Tuple2<Identifier, Identifier>, List<MigrateFilesInfo>>() {
            @Override
            public void processElement(
                    Tuple2<Identifier, Identifier> tuple,
                    ProcessFunction<Tuple2<Identifier, Identifier>, List<MigrateFilesInfo>>.Context
                            context,
                    Collector<List<MigrateFilesInfo>> collector)
                    throws Exception {
                // TODO: use metastore temporarily
                String sourceType = sourceCatalogConfig.get(CatalogOptions.METASTORE.key());
                checkNotNull(sourceType);
                try (Catalog sourceCatalog =
                                FlinkCatalogFactory.createPaimonCatalog(
                                        Options.fromMap(sourceCatalogConfig));
                        Catalog targetCatalog =
                                FlinkCatalogFactory.createPaimonCatalog(
                                        Options.fromMap(targetCatalogConfig))) {

                    switch (sourceType) {
                        case "hive":
                            Schema schema =
                                    HiveMigrateUtils.hiveTableToPaimonSchema(
                                            (HiveCatalog) sourceCatalog, tuple.f0);
                            Map<String, String> options = schema.options();
                            // only support Hive to unaware-bucket table now
                            options.put(CoreOptions.BUCKET.key(), "-1");
                            schema =
                                    new Schema(
                                            schema.fields(),
                                            schema.partitionKeys(),
                                            schema.primaryKeys(),
                                            options,
                                            schema.comment());
                            targetCatalog.createTable(tuple.f1, schema, false);
                            FileStoreTable table =
                                    (FileStoreTable) targetCatalog.getTable(tuple.f1);
                            List<HivePartitionFiles> allPartitions =
                                    HiveMigrateUtils.listHiveFiles(
                                            (HiveCatalog) sourceCatalog,
                                            tuple.f0,
                                            table.schema().logicalPartitionType(),
                                            table.coreOptions().partitionDefaultName());
                            List<MigrateFilesInfo> migrateFilesInfos = new ArrayList<>();
                            for (HivePartitionFiles partitionFiles : allPartitions) {
                                migrateFilesInfos.add(
                                        MigrateFilesInfo.fromHive(tuple.f1, partitionFiles, 0));
                            }
                            collector.collect(migrateFilesInfos);
                        default:
                            throw new UnsupportedOperationException(
                                    "Unsupported source type: " + sourceType);
                    }
                }
            }
        };
    }

    public static ProcessFunction<List<MigrateFilesInfo>, Void> copyAndCommitFunction(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        return new ProcessFunction<List<MigrateFilesInfo>, Void>() {

            @Override
            public void processElement(
                    List<MigrateFilesInfo> migrateFilesInfos,
                    ProcessFunction<List<MigrateFilesInfo>, Void>.Context context,
                    Collector<Void> collector)
                    throws Exception {
                try (Catalog targetCatalog =
                        FlinkCatalogFactory.createPaimonCatalog(
                                Options.fromMap(targetCatalogConfig))) {

                    // source FileIO
                    CatalogContext sourceContext =
                            CatalogContext.create(Options.fromMap(sourceCatalogConfig));
                    String warehouse = checkNotNull(sourceContext.options().get(WAREHOUSE));
                    FileIO sourceFileIO = FileIO.get(new Path(warehouse), sourceContext);

                    // group by table
                    Map<Identifier, List<MigrateFilesInfo>> groupedFiles = new HashMap<>();
                    for (MigrateFilesInfo files : migrateFilesInfos) {
                        groupedFiles
                                .computeIfAbsent(files.identifier(), k -> new ArrayList<>())
                                .add(files);
                    }

                    for (Map.Entry<Identifier, List<MigrateFilesInfo>> entry :
                            groupedFiles.entrySet()) {
                        FileStoreTable targetTable =
                                (FileStoreTable) targetCatalog.getTable(entry.getKey());
                        commit(entry.getValue(), sourceFileIO, targetTable);
                    }
                }
            }
        };
    }

    private static void commit(
            List<MigrateFilesInfo> migrateFilesInfos,
            FileIO sourceFileIO,
            FileStoreTable targetTable)
            throws Exception {
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (MigrateFilesInfo onePartitionFiles : migrateFilesInfos) {
            commitMessages.add(
                    copyFileAndGetCommitMessage(onePartitionFiles, sourceFileIO, targetTable));
        }
        try (BatchTableCommit commit = targetTable.newBatchWriteBuilder().newCommit()) {
            commit.commit(commitMessages);
        }
    }

    private static CommitMessage copyFileAndGetCommitMessage(
            MigrateFilesInfo migrateFilesInfo, FileIO sourceFileIO, FileStoreTable targetTable)
            throws IOException {
        // util for collecting stats
        CoreOptions options = targetTable.coreOptions();
        SimpleColStatsCollector.Factory[] factories =
                StatsCollectorFactories.createStatsFactories(
                        options.statsMode(), options, targetTable.rowType().getFieldNames());

        SimpleStatsExtractor simpleStatsExtractor =
                FileFormat.fromIdentifier(migrateFilesInfo.format(), options.toConfiguration())
                        .createStatsExtractor(targetTable.rowType(), factories)
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Can't get table stats extractor for format "
                                                        + migrateFilesInfo.format()));
        RowType rowTypeWithSchemaId =
                targetTable.schemaManager().schema(targetTable.schema().id()).logicalRowType();

        SimpleStatsConverter statsArraySerializer = new SimpleStatsConverter(rowTypeWithSchemaId);

        List<Path> paths = migrateFilesInfo.paths();
        List<Long> fileSizes = migrateFilesInfo.fileSizes();
        List<DataFileMeta> dataFileMetas = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            Path path = paths.get(i);
            long fileSize = fileSizes.get(i);

            // extract stats
            Pair<SimpleColStats[], SimpleStatsExtractor.FileInfo> fileInfo =
                    simpleStatsExtractor.extractWithFileInfo(sourceFileIO, path, fileSize);
            SimpleStats stats = statsArraySerializer.toBinaryAllMode(fileInfo.getLeft());

            // new file name
            String suffix = "." + migrateFilesInfo.format();
            String fileName = path.getName();
            String newFileName = fileName.endsWith(suffix) ? fileName : fileName + suffix;

            // copy files
            Path targetFilePath =
                    targetTable
                            .store()
                            .pathFactory()
                            .bucketPath(migrateFilesInfo.partition(), migrateFilesInfo.bucket());
            IOUtils.copyBytes(
                    sourceFileIO.newInputStream(path),
                    targetTable
                            .fileIO()
                            .newOutputStream(new Path(targetFilePath, newFileName), false));

            // to DataFileMeta
            DataFileMeta dataFileMeta =
                    DataFileMeta.forAppend(
                            newFileName,
                            fileSize,
                            fileInfo.getRight().getRowCount(),
                            stats,
                            0,
                            0,
                            targetTable.schema().id(),
                            Collections.emptyList(),
                            null,
                            FileSource.APPEND,
                            null,
                            null);
            dataFileMetas.add(dataFileMeta);
        }
        return FileMetaUtils.commitFile(
                migrateFilesInfo.partition(), targetTable.coreOptions().bucket(), dataFileMetas);
    }

    /** Files grouped by (table, partition, bucket) with necessary information. */
    public static class MigrateFilesInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Identifier identifier;
        private final BinaryRow partition;
        private final int bucket;
        private final List<Path> paths;
        private final List<Long> fileSizes;
        private final String format;

        public MigrateFilesInfo(
                Identifier identifier,
                BinaryRow partition,
                int bucket,
                List<Path> paths,
                List<Long> fileSizes,
                String format) {
            this.identifier = identifier;
            this.partition = partition;
            this.bucket = bucket;
            this.paths = paths;
            this.fileSizes = fileSizes;
            this.format = format;
        }

        public Identifier identifier() {
            return identifier;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public List<Path> paths() {
            return paths;
        }

        public List<Long> fileSizes() {
            return fileSizes;
        }

        public String format() {
            return format;
        }

        public static MigrateFilesInfo fromHive(
                Identifier identifier, HivePartitionFiles hivePartitionedFiles, int bucket) {
            return new MigrateFilesInfo(
                    identifier,
                    hivePartitionedFiles.partition(),
                    bucket,
                    hivePartitionedFiles.paths(),
                    hivePartitionedFiles.fileSizes(),
                    hivePartitionedFiles.format());
        }
    }

    /** Shuffle tables. */
    public static class TableChannelComputer
            implements ChannelComputer<Tuple2<Identifier, Identifier>> {

        private static final long serialVersionUID = 1L;

        private transient int numChannels;

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
        }

        @Override
        public int channel(Tuple2<Identifier, Identifier> record) {
            return Math.floorMod(
                    Objects.hash(record.f1.getDatabaseName(), record.f1.getTableName()),
                    numChannels);
        }

        @Override
        public String toString() {
            return "shuffle by identifier hash";
        }
    }
}
