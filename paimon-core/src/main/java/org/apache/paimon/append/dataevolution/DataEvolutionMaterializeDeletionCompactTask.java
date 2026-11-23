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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.RecordWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Physically applies deletion vectors while compacting a data evolution row-id range. */
public class DataEvolutionMaterializeDeletionCompactTask extends DataEvolutionCompactTask {

    private static final Logger LOG =
            LoggerFactory.getLogger(DataEvolutionMaterializeDeletionCompactTask.class);

    private final List<DeletionFile> deletionFiles;

    public DataEvolutionMaterializeDeletionCompactTask(
            BinaryRow partition, List<DataFileMeta> files, List<DeletionFile> deletionFiles) {
        super(partition, files);
        checkArgument(
                deletionFiles != null && deletionFiles.size() == files.size(),
                "Materialize deletion compact task should have deletion files aligned with data files.");
        this.deletionFiles = new ArrayList<>(deletionFiles);
    }

    public List<DeletionFile> deletionFiles() {
        return deletionFiles;
    }

    @Override
    public TaskType type() {
        return TaskType.MATERIALIZE_DELETION;
    }

    @Override
    public CommitMessage doCompact(FileStoreTable table, String commitUser) throws Exception {
        table = table.copy(DYNAMIC_WRITE_OPTIONS);
        FileStorePathFactory pathFactory = table.store().pathFactory();
        AppendOnlyFileStore store = (AppendOnlyFileStore) table.store();

        // build DataSplit with deletion vectors
        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(compactBefore)
                        .withDataDeletionFiles(deletionFiles)
                        .withBucketPath(pathFactory.bucketPath(partition, 0).toString())
                        .rawConvertible(false)
                        .build();
        RowType readType = table.rowType();
        List<ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (IndexedSplit split : readSplits(dataSplit)) {
            readers.add(
                    () -> store.newDataEvolutionRead().withReadType(readType).createReader(split));
        }
        RecordReader<InternalRow> reader = ConcatRecordReader.create(readers);
        @Nullable AppendFileStoreWrite storeWrite = null;
        @Nullable RecordWriter<InternalRow> writer = null;
        try {
            storeWrite = (AppendFileStoreWrite) store.newWrite(commitUser);
            storeWrite.withWriteType(table.rowType());
            writer = storeWrite.createWriter(partition, 0);
            RecordWriter<InternalRow> finalWriter = writer;
            reader.forEachRemaining(
                    row -> {
                        try {
                            finalWriter.write(row);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });

            compactAfter.addAll(writer.prepareCommit(false).newFilesIncrement().newFiles());
        } finally {
            try {
                reader.close();
            } finally {
                try {
                    if (writer != null) {
                        writer.close();
                    }
                } finally {
                    if (storeWrite != null) {
                        storeWrite.close();
                    }
                }
            }
        }

        return commitMessage(compactBefore, compactAfter);
    }

    static List<IndexedSplit> readSplits(DataSplit input) {
        List<IndexedSplit> splits = new ArrayList<>();
        List<DataFileMeta> inputFiles = input.dataFiles();
        List<DeletionFile> inputDeletions =
                input.deletionFiles().orElseGet(() -> Collections.nCopies(inputFiles.size(), null));
        for (Map.Entry<Range, List<Integer>> group : filesByNormalRange(inputFiles).entrySet()) {
            List<DataFileMeta> files = new ArrayList<>();
            List<DeletionFile> deletions = new ArrayList<>();
            for (int position : group.getValue()) {
                files.add(inputFiles.get(position));
                deletions.add(inputDeletions.get(position));
            }
            DataSplit split =
                    DataSplit.builder()
                            .withSnapshot(input.snapshotId())
                            .withPartition(input.partition())
                            .withBucket(input.bucket())
                            .withBucketPath(input.bucketPath())
                            .withDataFiles(files)
                            .withDataDeletionFiles(deletions)
                            .rawConvertible(false)
                            .build();
            splits.add(new IndexedSplit(split, Collections.singletonList(group.getKey()), null));
        }
        return splits;
    }

    private static NavigableMap<Range, List<Integer>> filesByNormalRange(
            List<DataFileMeta> inputFiles) {
        List<DataFileMeta> normalFiles =
                inputFiles.stream()
                        .filter(
                                file ->
                                        !isBlobFile(file.fileName())
                                                && !isVectorStoreFile(file.fileName()))
                        .collect(Collectors.toList());
        checkArgument(!normalFiles.isEmpty(), "Row range rewrite contains no normal files.");
        RangeHelper<DataFileMeta> helper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        NavigableMap<Range, List<Integer>> groups =
                new TreeMap<>(Comparator.comparingLong(range -> range.to));
        for (List<DataFileMeta> normalGroup : helper.mergeOverlappingRanges(normalFiles)) {
            checkArgument(
                    helper.areAllRangesSame(normalGroup),
                    "Normal data files %s should have the same row id range.",
                    normalGroup);
            groups.put(normalGroup.get(0).nonNullRowIdRange(), new ArrayList<>());
        }

        // Normal ranges are disjoint. Seek the first one whose end reaches the file's start,
        // then visit only ranges it spans. Keep manifest order and the matching DV position.
        for (int i = 0; i < inputFiles.size(); i++) {
            Range file = inputFiles.get(i).nonNullRowIdRange();
            Range firstEnd = new Range(file.from, file.from);
            for (Map.Entry<Range, List<Integer>> group :
                    groups.tailMap(firstEnd, true).entrySet()) {
                if (group.getKey().from > file.to) {
                    break;
                }
                group.getValue().add(i);
            }
        }
        return groups;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), deletionFiles);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o)
                && Objects.equals(
                        deletionFiles,
                        ((DataEvolutionMaterializeDeletionCompactTask) o).deletionFiles);
    }
}
