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
import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import static org.apache.paimon.types.BlobType.fieldNamesInBlobFile;
import static org.apache.paimon.types.VectorType.fieldNamesInVectorFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.checkContiguousRowRange;
import static org.apache.paimon.utils.DataEvolutionUtils.fieldMaxSequenceNumber;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compacts normal structured files of a data evolution table. */
public class DataEvolutionNormalCompactTask extends DataEvolutionCompactTask {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionNormalCompactTask.class);

    private final List<Range> protectedRanges;

    public DataEvolutionNormalCompactTask(BinaryRow partition, List<DataFileMeta> files) {
        this(partition, files, Collections.emptyList());
    }

    public DataEvolutionNormalCompactTask(
            BinaryRow partition, List<DataFileMeta> files, List<Range> protectedRanges) {
        super(partition, files);
        checkContiguousRowRange(files);
        this.protectedRanges =
                Collections.unmodifiableList(Range.sortAndMergeOverlap(protectedRanges));
    }

    public List<Range> protectedRanges() {
        return protectedRanges;
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other)
                && protectedRanges.equals(((DataEvolutionNormalCompactTask) other).protectedRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), protectedRanges);
    }

    @Override
    public TaskType type() {
        return TaskType.NORMAL;
    }

    @Override
    public CommitMessage doCompact(FileStoreTable table, String commitUser) throws Exception {
        CoreOptions options = table.coreOptions();

        if (isVectorStoreFile(compactBefore.get(0).fileName())) {
            // TODO: support vector-store file compaction
            throw new UnsupportedOperationException("Vector-store task is not supported");
        }

        Set<String> fieldsInDedicatedFile =
                SetUtils.union(
                        fieldNamesInBlobFile(table.rowType(), options.blobInlineField()),
                        fieldNamesInVectorFile(table.rowType(), options.withVectorFormat()));

        Map<String, String> writeOptions = new HashMap<>(DYNAMIC_WRITE_OPTIONS);
        if (options.dataEvolutionCompactionSplitLargeFiles()) {
            // Buffer flushes may close files before reaching a safe dedicated-file boundary.
            writeOptions.put(CoreOptions.WRITE_BUFFER_FOR_APPEND.key(), "false");
            writeOptions.put(
                    CoreOptions.TARGET_FILE_SIZE.key(), options.targetFileSize(false) + " b");
        }
        table = table.copy(writeOptions);
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
        storeWrite.withFileSource(FileSource.COMPACT);
        AppendOnlyWriter writer = (AppendOnlyWriter) storeWrite.createWriter(partition, 0);
        if (options.dataEvolutionCompactionSplitLargeFiles() && !protectedRanges.isEmpty()) {
            writer.withFileRollingPredicate(fileRollingPredicate(firstRowId));
        }

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
                options.dataEvolutionCompactionSplitLargeFiles() || writeResult.size() == 1,
                "Data evolution compaction should produce one file unless splitting is enabled.");

        try {
            writer.close();
            storeWrite.close();
        } catch (Exception e) {
            LOG.warn("Failed to close reader and writer.", e);
        }

        long minSequenceNumber = minSequenceId(compactBefore);
        long maxSequenceNumber = maxSequenceId(compactBefore);
        long nextRowId = firstRowId;
        long[] columnMaxSequenceNumbers =
                options.ignoreIndexColumnUpdate() && !writeResult.isEmpty()
                        ? compactedColumnMaxSequenceNumbers(
                                table,
                                writeResult
                                        .get(0)
                                        .assignSequenceNumber(minSequenceNumber, maxSequenceNumber))
                        : null;
        for (DataFileMeta file : writeResult) {
            DataFileMeta dataFileMeta =
                    file.assignFirstRowId(nextRowId)
                            .assignSequenceNumber(minSequenceNumber, maxSequenceNumber);
            if (columnMaxSequenceNumbers != null) {
                dataFileMeta = dataFileMeta.withColumnMaxSequenceNumbers(columnMaxSequenceNumbers);
            }
            compactAfter.add(dataFileMeta);
            nextRowId += dataFileMeta.rowCount();
        }
        checkSameRowRange("Normal file", compactBefore, compactAfter);

        return commitMessage(compactBefore, compactAfter);
    }

    private LongPredicate fileRollingPredicate(long firstRowId) {
        Iterator<Range> ranges = protectedRanges.iterator();
        return new LongPredicate() {
            private Range current = ranges.hasNext() ? ranges.next() : null;

            @Override
            public boolean test(long writtenRows) {
                long lastRowId = firstRowId + (writtenRows - 1);
                while (current != null && current.to <= lastRowId) {
                    current = ranges.hasNext() ? ranges.next() : null;
                }
                return current == null || lastRowId < current.from;
            }
        };
    }

    @Nullable
    private long[] compactedColumnMaxSequenceNumbers(
            FileStoreTable table, DataFileMeta outputFile) {
        SchemaManager schemaManager = table.schemaManager();
        Map<Long, TableSchema> schemaCache = new HashMap<>();
        Function<Long, TableSchema> schemaLoader =
                schemaId -> schemaCache.computeIfAbsent(schemaId, schemaManager::schema);
        Map<Pair<Long, List<String>>, List<DataField>> fileFieldsCache = new HashMap<>();

        Map<Integer, Long> fieldMaxSequences = new HashMap<>();
        for (DataFileMeta input : compactBefore) {
            List<DataField> inputFields =
                    fileFieldsCache.computeIfAbsent(
                            Pair.of(input.schemaId(), input.writeCols()),
                            key -> fileFields(schemaLoader, input));
            long[] inputColumnSequences = input.columnMaxSequenceNumbers();
            for (int inputPosition = 0; inputPosition < inputFields.size(); inputPosition++) {
                fieldMaxSequences.merge(
                        inputFields.get(inputPosition).id(),
                        fieldMaxSequenceNumber(
                                input, inputColumnSequences, inputPosition, inputFields.size()),
                        Math::max);
            }
        }

        long fallbackSequence = outputFile.maxSequenceNumber();
        List<DataField> outputFields =
                fileFieldsCache.computeIfAbsent(
                        Pair.of(outputFile.schemaId(), outputFile.writeCols()),
                        key -> fileFields(schemaLoader, outputFile));
        boolean allEqualToFileMax =
                outputFields.stream()
                        .allMatch(
                                field ->
                                        fieldMaxSequences.getOrDefault(field.id(), fallbackSequence)
                                                == fallbackSequence);
        if (allEqualToFileMax) {
            return null;
        }

        long[] result = new long[outputFields.size()];
        for (int outputPosition = 0; outputPosition < outputFields.size(); outputPosition++) {
            result[outputPosition] =
                    fieldMaxSequences.getOrDefault(
                            outputFields.get(outputPosition).id(), fallbackSequence);
        }
        return result;
    }

    private static List<DataField> fileFields(
            Function<Long, TableSchema> schemaLoader, DataFileMeta file) {
        TableSchema fileSchema = schemaLoader.apply(file.schemaId());
        return org.apache.paimon.utils.DataEvolutionUtils.fileFields(fileSchema, file);
    }
}
