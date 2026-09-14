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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
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
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
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
            writeOptions.put(CoreOptions.TARGET_FILE_SIZE.key(), Long.MAX_VALUE + " b");
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
        RecordWriter<InternalRow> writer = storeWrite.createWriter(partition, 0);
        List<Range> outputRanges =
                options.dataEvolutionCompactionSplitLargeFiles()
                        ? planOutputRanges(options.targetFileSize(false))
                        : Collections.singletonList(checkContiguousRowRange(compactBefore));
        List<DataFileMeta> writeResult = new ArrayList<>();
        try (RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            for (Range range : outputRanges) {
                for (long remaining = range.count(); remaining > 0; remaining--) {
                    checkArgument(iterator.hasNext(), "Missing rows in normal compaction input.");
                    writer.write(iterator.next());
                }
                List<DataFileMeta> output =
                        writer.prepareCommit(false).newFilesIncrement().newFiles();
                checkArgument(
                        output.size() == 1,
                        "Each planned compaction range should produce one normal file.");
                writeResult.add(output.get(0));
            }
            checkArgument(!iterator.hasNext(), "Unexpected extra rows in normal compaction input.");
        }
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

    @VisibleForTesting
    List<Range> planOutputRanges(long targetFileSize) {
        Range inputRange = checkContiguousRowRange(compactBefore);
        double inputSize = compactBefore.stream().mapToDouble(DataFileMeta::fileSize).sum();
        long targetRows = Math.max(1L, (long) (inputRange.count() * (targetFileSize / inputSize)));
        List<Range> result = new ArrayList<>();
        int protectedIndex = 0;
        long start = inputRange.from;
        while (true) {
            long end = start + Math.min(targetRows - 1, inputRange.to - start);
            while (protectedIndex < protectedRanges.size()
                    && protectedRanges.get(protectedIndex).to <= end) {
                protectedIndex++;
            }
            if (protectedIndex < protectedRanges.size()
                    && protectedRanges.get(protectedIndex).from <= end) {
                end = protectedRanges.get(protectedIndex).to;
            }
            checkArgument(
                    end <= inputRange.to,
                    "Dedicated range must be contained in the normal compaction range.");
            result.add(new Range(start, end));
            if (end == inputRange.to) {
                return result;
            }
            start = end + 1;
        }
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
