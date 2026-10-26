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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.StatsCollectorFactories;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.types.VectorType.fieldsInVectorFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.checkContiguousRowRange;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Compacts the dedicated vector-store files of one normal row-ID range of a data evolution table.
 *
 * <p>The task must receive every vector-store file of that range. The rewritten files take the
 * highest sequence number of the inputs, so a vector-store file left out of the task could start or
 * stop winning rows against them.
 */
public class DataEvolutionVectorCompactTask extends DataEvolutionCompactTask {

    public DataEvolutionVectorCompactTask(BinaryRow partition, List<DataFileMeta> files) {
        super(partition, files);
        checkArgument(
                files.stream().allMatch(file -> isVectorStoreFile(file.fileName())),
                "Vector compaction task %s should only contain vector-store files.",
                files);
        checkContiguousRowRange(files);
    }

    @Override
    public TaskType type() {
        return TaskType.VECTOR;
    }

    /**
     * Whether every current vector field of {@code files} is present in some file for every row of
     * their combined range. Otherwise the rewrite would store NULL for rows that never had the
     * field. Fields dropped from the table are ignored because the rewrite does not keep them; a
     * null {@code currentFieldIds} checks every field.
     */
    static boolean coversRangeForEveryField(
            List<DataFileMeta> files,
            LongFunction<RowType> schemaFetcher,
            @Nullable Set<Integer> currentFieldIds) {
        List<Range> merged =
                Range.sortAndMergeOverlap(
                        files.stream()
                                .map(DataFileMeta::nonNullRowIdRange)
                                .collect(Collectors.toList()),
                        true);
        if (merged.size() != 1) {
            return false;
        }

        Map<Integer, List<Range>> fieldRanges = new HashMap<>();
        Map<Long, RowType> schemaCache = new HashMap<>();
        for (DataFileMeta file : files) {
            for (int fieldId : fieldIds(file, schemaCache, schemaFetcher)) {
                if (currentFieldIds == null || currentFieldIds.contains(fieldId)) {
                    fieldRanges
                            .computeIfAbsent(fieldId, ignored -> new ArrayList<>())
                            .add(file.nonNullRowIdRange());
                }
            }
        }
        if (fieldRanges.isEmpty()) {
            return false;
        }
        for (List<Range> ranges : fieldRanges.values()) {
            if (!Range.sortAndMergeOverlap(ranges, true).equals(merged)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public CommitMessage doCompact(FileStoreTable table, String commitUser) throws Exception {
        CoreOptions options = table.coreOptions();
        List<DataFileMeta> sortedCompactBefore = sortedByFirstRowId(compactBefore);
        checkArgument(
                sortedCompactBefore.size() > 1,
                "Vector compaction task %s should contain at least two files to compact.",
                this);
        FileFormat vectorFileFormat =
                checkNotNull(
                        FileFormat.vectorFileFormat(options),
                        "Vector compaction requires option '%s'.",
                        CoreOptions.VECTOR_FILE_FORMAT.key());
        LongFunction<RowType> schemaFetcher =
                schemaId -> table.schemaManager().schema(schemaId).logicalRowType();
        List<DataField> currentFields = fieldsInVectorFile(table.rowType(), true);
        checkArgument(
                coversRangeForEveryField(
                        sortedCompactBefore, schemaFetcher, fieldIds(currentFields)),
                "Every current vector field of compaction task %s should cover the whole row range.",
                this);
        Range compactBeforeRange = checkContiguousRowRange(sortedCompactBefore);
        RowType writeType =
                new RowType(inputFields(currentFields, sortedCompactBefore, schemaFetcher));

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
                store.newDataEvolutionRead().withReadType(writeType).createReader(dataSplit);
        RollingFileWriterImpl<InternalRow, DataFileMeta> writer =
                createVectorFileWriter(table, options, vectorFileFormat, writeType, pathFactory);

        long rowCount = 0;
        try (RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            while (iterator.hasNext()) {
                writer.write(iterator.next());
                rowCount++;
            }
            writer.close();
        } catch (Exception e) {
            writer.abort();
            throw e;
        }
        checkArgument(
                rowCount == compactBeforeRange.count(),
                "Vector compaction read %s rows, but the compacted range %s has %s rows.",
                rowCount,
                compactBeforeRange,
                compactBeforeRange.count());

        long minSequenceNumber = minSequenceId(sortedCompactBefore);
        long maxSequenceNumber = maxSequenceId(sortedCompactBefore);
        long nextRowId = compactBeforeRange.from;
        List<DataFileMeta> writeResult = writer.result();
        long[] writeColsSequences =
                options.ignoreIndexColumnUpdate() && !writeResult.isEmpty()
                        ? compactedWriteColsSequences(
                                table,
                                writeResult
                                        .get(0)
                                        .assignSequenceNumber(minSequenceNumber, maxSequenceNumber))
                        : null;
        for (DataFileMeta file : writeResult) {
            DataFileMeta compacted =
                    file.assignFirstRowId(nextRowId)
                            .assignSequenceNumber(minSequenceNumber, maxSequenceNumber);
            if (writeColsSequences != null) {
                compacted = compacted.withWriteColsSequences(writeColsSequences);
            }
            compactAfter.add(compacted);
            nextRowId += compacted.rowCount();
        }
        checkSameRowRange("Vector compact files", sortedCompactBefore, compactAfter);

        return commitMessage(sortedCompactBefore, compactAfter);
    }

    private RollingFileWriterImpl<InternalRow, DataFileMeta> createVectorFileWriter(
            FileStoreTable table,
            CoreOptions options,
            FileFormat vectorFileFormat,
            RowType writeType,
            DataFilePathFactory pathFactory) {
        List<String> fieldNames = writeType.getFieldNames();
        StatsCollectorFactories statsCollectorFactories = new StatsCollectorFactories(options);
        return new RollingFileWriterImpl<>(
                () ->
                        new RowDataFileWriter(
                                table.fileIO(),
                                RollingFileWriter.createFileWriterContext(
                                        vectorFileFormat,
                                        writeType,
                                        statsCollectorFactories.statsCollectors(fieldNames),
                                        options.fileCompression()),
                                pathFactory.newVectorPath(vectorFileFormat.getFormatIdentifier()),
                                writeType,
                                table.schema().id(),
                                () -> new LongCounter(0),
                                new FileIndexOptions(),
                                FileSource.COMPACT,
                                false,
                                options.statsDenseStore(),
                                pathFactory.isExternalPath(),
                                fieldNames,
                                null,
                                null),
                options.vectorTargetFileSize(),
                Long.MAX_VALUE);
    }

    /** Current vector-store fields written by any input, in table order. */
    private static List<DataField> inputFields(
            List<DataField> currentFields,
            List<DataFileMeta> files,
            LongFunction<RowType> schemaFetcher) {
        Set<Integer> inputFieldIds = new HashSet<>();
        Map<Long, RowType> schemaCache = new HashMap<>();
        for (DataFileMeta file : files) {
            inputFieldIds.addAll(fieldIds(file, schemaCache, schemaFetcher));
        }
        return currentFields.stream()
                .filter(field -> inputFieldIds.contains(field.id()))
                .collect(Collectors.toList());
    }

    static Set<Integer> fieldIds(List<DataField> fields) {
        return fields.stream().map(DataField::id).collect(Collectors.toSet());
    }

    private static List<Integer> fieldIds(
            DataFileMeta file,
            Map<Long, RowType> schemaCache,
            LongFunction<RowType> schemaFetcher) {
        checkArgument(
                file.writeCols() != null && !file.writeCols().isEmpty(),
                "Vector-store file %s should record its write columns.",
                file);
        RowType rowType = schemaCache.computeIfAbsent(file.schemaId(), schemaFetcher::apply);
        return file.writeCols().stream()
                .map(name -> rowType.getField(name).id())
                .collect(Collectors.toList());
    }
}
