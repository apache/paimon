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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexCommon;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.flink.procedure.FileIndexProcedure;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.FileIndexWriter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFilePathFactory.fileIndexPathIncrease;
import static org.apache.paimon.io.DataFilePathFactory.toFileIndexPath;

/** File index sink for {@link FileIndexProcedure}. */
public class FileIndexSink extends FlinkWriteSink<ManifestEntry> {

    public FileIndexSink(FileStoreTable table) {
        super(table, null);
    }

    @Override
    protected OneInputStreamOperator<ManifestEntry, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new FileIndexModificationOperator(table.coreOptions().toConfiguration(), table);
    }

    /** File index modification operator to rewrite file index. */
    private static class FileIndexModificationOperator
            extends PrepareCommitOperator<ManifestEntry, Committable> {

        private final FileStoreTable table;

        private transient FileIndexProcessor fileIndexProcessor;
        private transient List<CommitMessage> messages;

        public FileIndexModificationOperator(Options options, FileStoreTable table) {
            super(options);
            this.table = table;
        }

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<Committable>> output) {
            super.setup(containingTask, config, output);

            this.fileIndexProcessor = new FileIndexProcessor(table);
            this.messages = new ArrayList<>();
        }

        @Override
        public void processElement(StreamRecord<ManifestEntry> element) throws Exception {
            ManifestEntry entry = element.getValue();
            BinaryRow partition = entry.partition();
            int bucket = entry.bucket();
            DataFileMeta file = entry.file();
            DataFileMeta indexedFile = fileIndexProcessor.process(partition, bucket, file);

            CommitMessageImpl commitMessage =
                    new CommitMessageImpl(
                            partition,
                            bucket,
                            DataIncrement.emptyIncrement(),
                            new CompactIncrement(
                                    Collections.singletonList(file),
                                    Collections.singletonList(indexedFile),
                                    Collections.emptyList()));

            messages.add(commitMessage);
        }

        @Override
        protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
                throws IOException {
            ArrayList<CommitMessage> temp = new ArrayList<>(messages);
            messages.clear();
            return temp.stream()
                    .map(s -> new Committable(checkpointId, Committable.Kind.FILE, s))
                    .collect(Collectors.toList());
        }
    }

    /** Does the file index rewrite. */
    public static class FileIndexProcessor {

        private final FileStoreTable table;
        private final FileIndexOptions fileIndexOptions;
        private final FileIO fileIO;
        private final FileStorePathFactory pathFactory;
        private final Map<Pair<BinaryRow, Integer>, DataFilePathFactory> dataFilePathFactoryMap;
        private final SchemaCache schemaCache;
        private final long sizeInMeta;

        public FileIndexProcessor(FileStoreTable table) {
            this.table = table;
            this.fileIndexOptions = table.coreOptions().indexColumnsOptions();
            this.fileIO = table.fileIO();
            this.pathFactory = table.store().pathFactory();
            this.dataFilePathFactoryMap = new HashMap<>();
            this.schemaCache =
                    new SchemaCache(fileIndexOptions, new SchemaManager(fileIO, table.location()));
            this.sizeInMeta = table.coreOptions().fileIndexInManifestThreshold();
        }

        public DataFileMeta process(BinaryRow partition, int bucket, DataFileMeta dataFileMeta)
                throws IOException {
            DataFilePathFactory dataFilePathFactory =
                    dataFilePathFactoryMap.computeIfAbsent(
                            Pair.of(partition, bucket),
                            p -> pathFactory.createDataFilePathFactory(partition, bucket));

            Tuple4<RowType, Map<String, String>, int[], Map<String, Set<String>>> t4 =
                    schemaCache.schemaInfo(dataFileMeta.schemaId());
            RowType fileSchema = t4.f0;
            Map<String, String> evolutionNameMap = t4.f1;
            int[] projection = t4.f2;
            Map<String, Set<String>> expectedFileIndex = t4.f3;

            List<String> extras = new ArrayList<>(dataFileMeta.extraFiles());
            List<String> indexFiles =
                    dataFileMeta.extraFiles().stream()
                            .filter(name -> name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());
            extras.removeAll(indexFiles);

            Path newIndexPath;
            Map<String, Map<String, byte[]>> maintainers;
            // load
            if (!indexFiles.isEmpty()) {
                String indexFile = indexFiles.get(0);
                try (FileIndexFormat.Reader indexReader =
                        FileIndexFormat.createReader(
                                fileIO.newInputStream(dataFilePathFactory.toPath(indexFile)),
                                fileSchema)) {
                    maintainers = indexReader.readAll();
                }
                newIndexPath = fileIndexPathIncrease(dataFilePathFactory.toPath(indexFile));
            } else {
                maintainers = new HashMap<>();
                newIndexPath = toFileIndexPath(dataFilePathFactory.toPath(dataFileMeta.fileName()));
            }

            // remove unnecessary
            for (Map.Entry<String, Map<String, byte[]>> entry :
                    new HashSet<>(maintainers.entrySet())) {
                String name = entry.getKey();
                if (!expectedFileIndex.containsKey(name)) {
                    maintainers.remove(name);
                } else {
                    Map<String, byte[]> indexTypeBytes = maintainers.get(name);
                    for (String indexType : entry.getValue().keySet()) {
                        if (!indexTypeBytes.containsKey(indexType)) {
                            indexTypeBytes.remove(indexType);
                        }
                    }
                }
            }

            // ignore close
            FileIndexWriter fileIndexWriter =
                    FileIndexWriter.create(
                            fileIO,
                            newIndexPath,
                            fileSchema.project(projection),
                            fileIndexOptions,
                            evolutionNameMap);
            if (fileIndexWriter != null) {
                try (RecordReader<InternalRow> reader =
                        table.newReadBuilder()
                                .withProjection(projection)
                                .newRead()
                                .createReader(
                                        DataSplit.builder()
                                                .withPartition(partition)
                                                .withBucket(bucket)
                                                .withBucketPath(
                                                        pathFactory
                                                                .bucketPath(partition, bucket)
                                                                .toString())
                                                .withDataFiles(
                                                        Collections.singletonList(dataFileMeta))
                                                .rawConvertible(true)
                                                .build())) {
                    reader.forEachRemaining(fileIndexWriter::write);
                }

                fileIndexWriter
                        .serializeMaintainers()
                        .forEach(
                                (key, value) ->
                                        maintainers
                                                .computeIfAbsent(key, k -> new HashMap<>())
                                                .putAll(value));
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (FileIndexFormat.Writer indexWriter = FileIndexFormat.createWriter(baos)) {
                if (!maintainers.isEmpty()) {
                    indexWriter.writeColumnIndexes(maintainers);
                }
            }

            if (baos.size() > sizeInMeta) {
                try (OutputStream outputStream = fileIO.newOutputStream(newIndexPath, true)) {
                    outputStream.write(baos.toByteArray());
                }
                extras.add(newIndexPath.getName());
                return dataFileMeta.copy(extras);
            } else if (baos.size() == 0) {
                return dataFileMeta.copy(extras);
            } else {
                return dataFileMeta.copy(baos.toByteArray());
            }
        }
    }

    /** Schema id to specified information related to schema. */
    private static class SchemaCache {

        private final FileIndexOptions fileIndexOptions;
        private final SchemaManager schemaManager;
        private final TableSchema latest;
        private final Map<
                        Long, Tuple4<RowType, Map<String, String>, int[], Map<String, Set<String>>>>
                schemaInfos;

        private final Set<Long> fileSchemaIds;

        public SchemaCache(FileIndexOptions fileIndexOptions, SchemaManager schemaManager) {
            this.fileIndexOptions = fileIndexOptions;
            this.schemaManager = schemaManager;
            this.latest = schemaManager.latest().orElseThrow(RuntimeException::new);
            this.schemaInfos = new HashMap<>();
            this.fileSchemaIds = new HashSet<>();
        }

        public Tuple4<RowType, Map<String, String>, int[], Map<String, Set<String>>> schemaInfo(
                long schemaId) {
            if (!fileSchemaIds.contains(schemaId)) {
                RowType fileSchema = schemaManager.schema(schemaId).logicalRowType();
                Map<String, String> evolutionmap =
                        schemaId == latest.id()
                                ? null
                                : createIndexNameMapping(latest.fields(), fileSchema.getFields());

                List<String> projectedColumnNames = new ArrayList<>();
                Map<String, Set<String>> expectedIndexNameType = new HashMap<>();
                for (Map.Entry<FileIndexOptions.Column, Map<String, Options>> entry :
                        fileIndexOptions.entrySet()) {
                    FileIndexOptions.Column column = entry.getKey();
                    String columnName;
                    if (evolutionmap != null) {
                        columnName = evolutionmap.getOrDefault(column.getColumnName(), null);
                        // if column name has no corresponding field, then we just skip it
                        if (columnName == null) {
                            continue;
                        }
                    } else {
                        columnName = column.getColumnName();
                    }
                    projectedColumnNames.add(columnName);
                    String fullColumnName =
                            column.isNestedColumn()
                                    ? FileIndexCommon.toMapKey(
                                            columnName, column.getNestedColumnName())
                                    : column.getColumnName();
                    expectedIndexNameType
                            .computeIfAbsent(fullColumnName, name -> new HashSet<>())
                            .addAll(entry.getValue().keySet());
                }

                schemaInfos.put(
                        schemaId,
                        Tuple4.of(
                                fileSchema,
                                evolutionmap,
                                projectedColumnNames.stream()
                                        .mapToInt(fileSchema::getFieldIndex)
                                        .toArray(),
                                expectedIndexNameType));
                fileSchemaIds.add(schemaId);
            }

            return schemaInfos.get(schemaId);
        }

        private static Map<String, String> createIndexNameMapping(
                List<DataField> tableFields, List<DataField> dataFields) {
            Map<String, String> indexMapping = new HashMap<>();
            Map<Integer, String> fieldIdToIndex = new HashMap<>();
            for (DataField dataField : tableFields) {
                fieldIdToIndex.put(dataField.id(), dataField.name());
            }

            for (DataField tableField : dataFields) {
                String dataFieldIndex = fieldIdToIndex.getOrDefault(tableField.id(), null);
                if (dataFieldIndex != null) {
                    indexMapping.put(dataFieldIndex, tableField.name());
                }
            }

            return indexMapping;
        }
    }
}
