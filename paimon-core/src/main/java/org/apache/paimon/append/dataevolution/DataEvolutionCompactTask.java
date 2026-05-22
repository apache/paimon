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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.FileWriter;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;
import static org.apache.paimon.types.BlobType.fieldNamesInBlobFile;
import static org.apache.paimon.types.VectorType.fieldNamesInVectorFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Data evolution table compaction task. */
public class DataEvolutionCompactTask extends AppendCompactTask {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionCompactTask.class);

    private static final Map<String, String> DYNAMIC_WRITE_OPTIONS = dynamicWriteOptions();

    private static Map<String, String> dynamicWriteOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "99999 G");
        options.put(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "99999 G");
        return Collections.unmodifiableMap(options);
    }

    private final boolean blobTask;

    public DataEvolutionCompactTask(
            BinaryRow partition, List<DataFileMeta> files, boolean blobTask) {
        super(partition, files);
        this.blobTask = blobTask;
    }

    public boolean isBlobTask() {
        return blobTask;
    }

    public CommitMessage doCompact(FileStoreTable table, String commitUser) throws Exception {
        if (blobTask) {
            return doCompactBlobFiles(table, commitUser);
        }
        if (isVectorStoreFile(compactBefore.get(0).fileName())) {
            // TODO: support vector-store file compaction
            throw new UnsupportedOperationException("Vector-store task is not supported");
        }

        CoreOptions options = table.coreOptions();

        Set<String> fieldsInDedicatedFile =
                SetUtils.union(
                        fieldNamesInBlobFile(table.rowType(), options.blobInlineField()),
                        fieldNamesInVectorFile(table.rowType(), options.withVectorFormat()));

        table = table.copy(DYNAMIC_WRITE_OPTIONS);
        long firstRowId = compactBefore.get(0).nonNullFirstRowId();

        RowType readWriteType =
                new RowType(
                        table.rowType().getFields().stream()
                                .filter(f -> !fieldsInDedicatedFile.contains(f.name()))
                                .collect(Collectors.toList()));
        FileStorePathFactory pathFactory = table.store().pathFactory();
        AppendOnlyFileStore store = (AppendOnlyFileStore) table.store();

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(compactBefore)
                        .withBucketPath(pathFactory.bucketPath(partition, 0).toString())
                        .rawConvertible(false)
                        .build();
        RecordReader<InternalRow> reader =
                store.newDataEvolutionRead().withReadType(readWriteType).createReader(dataSplit);
        AppendFileStoreWrite storeWrite = (AppendFileStoreWrite) store.newWrite(commitUser);
        storeWrite.withWriteType(readWriteType);
        RecordWriter<InternalRow> writer = storeWrite.createWriter(partition, 0);

        reader.forEachRemaining(
                row -> {
                    try {
                        writer.write(row);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        List<DataFileMeta> writeResult = writer.prepareCommit(false).newFilesIncrement().newFiles();
        checkArgument(
                writeResult.size() == 1, "Data evolution compaction should produce one file.");

        try {
            writer.close();
            storeWrite.close();
        } catch (Exception e) {
            LOG.warn("Failed to close reader and writer.", e);
        }

        DataFileMeta dataFileMeta = writeResult.get(0).assignFirstRowId(firstRowId);
        long minSequenceId =
                compactBefore.stream()
                        .mapToLong(DataFileMeta::minSequenceNumber)
                        .min()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Cannot get min sequence id from compact before files."));
        long maxSequenceId =
                compactBefore.stream()
                        .mapToLong(DataFileMeta::maxSequenceNumber)
                        .max()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Cannot get max sequence id from compact before files."));
        dataFileMeta = dataFileMeta.assignSequenceNumber(minSequenceId, maxSequenceId);
        compactAfter.add(dataFileMeta);

        CompactIncrement compactIncrement =
                new CompactIncrement(
                        compactBefore,
                        compactAfter,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());
        return new CommitMessageImpl(
                partition, 0, null, DataIncrement.emptyIncrement(), compactIncrement);
    }

    private CommitMessage doCompactBlobFiles(FileStoreTable table, String commitUser)
            throws Exception {
        CoreOptions options = table.coreOptions();
        List<DataFileMeta> sortedCompactBefore = sortedByFirstRowId(compactBefore);
        DataField blobField = blobField(table, options, sortedCompactBefore);
        checkRowIdsContinuous(sortedCompactBefore, "Blob compact before files");
        checkArgument(
                sortedCompactBefore.size() > 1,
                "Blob compaction task %s should contain at least two files to compact.",
                this);

        RowType blobWriteType = new RowType(Collections.singletonList(blobField));

        AppendOnlyFileStore store = (AppendOnlyFileStore) table.store();
        DataFilePathFactory pathFactory =
                store.pathFactory().createDataFilePathFactory(partition, 0);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(sortedCompactBefore)
                        .withBucketPath(pathFactory.parent().toString())
                        .rawConvertible(false)
                        .build();
        RecordReader<InternalRow> reader =
                store.newDataEvolutionRead().withReadType(blobWriteType).createReader(dataSplit);
        FileWriter<InternalRow, DataFileMeta> writer =
                createBlobFileWriter(table, options, blobWriteType, blobField.name(), pathFactory);

        try {
            reader.forEachRemaining(
                    row -> {
                        try {
                            writer.write(row);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            writer.close();
        } catch (Exception e) {
            writer.abort();
            throw e;
        }

        long firstRowId = sortedCompactBefore.get(0).nonNullFirstRowId();
        long minSequenceId = minSequenceId(sortedCompactBefore);
        long maxSequenceId = maxSequenceId(sortedCompactBefore);
        DataFileMeta compactedFile =
                writer.result()
                        .assignFirstRowId(firstRowId)
                        .assignSequenceNumber(minSequenceId, maxSequenceId);
        compactAfter.add(compactedFile);
        checkArgument(
                !compactAfter.isEmpty(), "Blob file compaction should produce at least one file.");
        checkRowIdsContinuous(compactAfter, "Blob compact after files");

        CompactIncrement compactIncrement =
                new CompactIncrement(
                        sortedCompactBefore,
                        compactAfter,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());
        return new CommitMessageImpl(
                partition, 0, null, DataIncrement.emptyIncrement(), compactIncrement);
    }

    private FileWriter<InternalRow, DataFileMeta> createBlobFileWriter(
            FileStoreTable table,
            CoreOptions options,
            RowType blobWriteType,
            String blobFieldName,
            DataFilePathFactory pathFactory) {
        BlobFileFormat blobFileFormat = new BlobFileFormat();
        return new RowDataFileWriter(
                table.fileIO(),
                RollingFileWriter.createFileWriterContext(
                        blobFileFormat,
                        blobWriteType,
                        new SimpleColStatsCollector.Factory[] {NoneSimpleColStatsCollector::new},
                        "none"),
                pathFactory.newBlobPath(),
                blobWriteType,
                table.schema().id(),
                () -> new LongCounter(0),
                new FileIndexOptions(),
                FileSource.COMPACT,
                false,
                options.statsDenseStore(),
                pathFactory.isExternalPath(),
                Collections.singletonList(blobFieldName));
    }

    private List<DataFileMeta> sortedByFirstRowId(List<DataFileMeta> files) {
        List<DataFileMeta> sorted = new ArrayList<>(files);
        sorted.sort(comparingLong(DataFileMeta::nonNullFirstRowId));
        return sorted;
    }

    private DataField blobField(
            FileStoreTable table, CoreOptions options, List<DataFileMeta> files) {
        Integer blobFieldId = null;
        for (DataFileMeta file : files) {
            checkArgument(
                    file.writeCols() != null && file.writeCols().size() == 1,
                    "Blob file %s should contain exactly one write column.",
                    file);
            RowType fileRowType = table.schemaManager().schema(file.schemaId()).logicalRowType();
            int currentFieldId = fileRowType.getField(file.writeCols().get(0)).id();
            if (blobFieldId == null) {
                blobFieldId = currentFieldId;
            } else {
                checkArgument(
                        blobFieldId == currentFieldId,
                        "Blob compact before files %s should contain the same field.",
                        files);
            }
        }

        checkArgument(blobFieldId != null, "Blob compaction task should not be empty.");
        checkArgument(
                table.rowType().containsField(blobFieldId),
                "Cannot find blob field id %s in latest schema for compaction task %s.",
                blobFieldId,
                this);
        DataField field = table.rowType().getField(blobFieldId);
        Set<String> blobFieldNames =
                fieldNamesInBlobFile(table.rowType(), options.blobInlineField());
        checkArgument(
                blobFieldNames.contains(field.name()),
                "Field %s in latest schema is not a blob file field.",
                field.name());
        return field;
    }

    private void checkRowIdsContinuous(List<DataFileMeta> files, String description) {
        checkArgument(!files.isEmpty(), "%s should not be empty.", description);
        long expectedFirstRowId = files.get(0).nonNullFirstRowId();
        for (DataFileMeta file : files) {
            long firstRowId = file.nonNullFirstRowId();
            checkArgument(
                    firstRowId == expectedFirstRowId,
                    "%s should be continuous and sorted by row id, expected %s but got %s in file %s.",
                    description,
                    expectedFirstRowId,
                    firstRowId,
                    file);
            expectedFirstRowId += file.rowCount();
        }
    }

    private long minSequenceId(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(DataFileMeta::minSequenceNumber)
                .min()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Cannot get min sequence id from compact before files."));
    }

    private long maxSequenceId(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(DataFileMeta::maxSequenceNumber)
                .max()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Cannot get max sequence id from compact before files."));
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, compactBefore, compactAfter, blobTask);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataEvolutionCompactTask that = (DataEvolutionCompactTask) o;
        return blobTask == that.blobTask
                && Objects.equals(partition, that.partition)
                && Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter);
    }

    @Override
    public String toString() {
        return String.format(
                "DataEvolutionCompactTask {"
                        + "partition = %s, "
                        + "compactBefore = %s, "
                        + "compactAfter = %s, "
                        + "blobTask = %s}",
                partition, compactBefore, compactAfter, blobTask);
    }
}
