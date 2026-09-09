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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
        if (compactBefore.stream().anyMatch(file -> isVectorStoreFile(file.fileName()))) {
            // TODO: support vector-store file compaction
            throw new UnsupportedOperationException("Vector-store task is not supported");
        }

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
        RecordReader<InternalRow> reader = createReader(store, table.rowType(), dataSplit);
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

    private RecordReader<InternalRow> createReader(
            AppendOnlyFileStore store, RowType readType, DataSplit dataSplit) throws IOException {
        List<DataFileMeta> normalFiles = new ArrayList<>();
        for (DataFileMeta file : compactBefore) {
            if (!isBlobFile(file.fileName()) && !isVectorStoreFile(file.fileName())) {
                normalFiles.add(file);
            }
        }

        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        List<ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (List<DataFileMeta> normalGroup : rangeHelper.mergeOverlappingRanges(normalFiles)) {
            checkArgument(
                    rangeHelper.areAllRangesSame(normalGroup),
                    "Normal data files %s should have the same row id range.",
                    normalGroup);
            Range range = normalGroup.get(0).nonNullRowIdRange();
            Set<DataFileMeta> normalGroupFiles = new HashSet<>(normalGroup);
            DataSplit rangeSplit =
                    dataSplit
                            .filterDataFile(
                                    file ->
                                            normalGroupFiles.contains(file)
                                                    || ((isBlobFile(file.fileName())
                                                                    || isVectorStoreFile(
                                                                            file.fileName()))
                                                            && file.nonNullRowIdRange()
                                                                    .hasIntersection(range)))
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Cannot find files for row id range " + range));
            IndexedSplit indexedSplit =
                    new IndexedSplit(rangeSplit, Collections.singletonList(range), null);
            readers.add(
                    () ->
                            store.newDataEvolutionRead()
                                    .withReadType(readType)
                                    .createReader(indexedSplit));
        }
        checkArgument(!readers.isEmpty(), "Materialize deletion task contains no normal files.");
        return ConcatRecordReader.create(readers);
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
