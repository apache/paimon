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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RecordWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
        RecordReader<InternalRow> reader =
                store.newDataEvolutionRead().withReadType(table.rowType()).createReader(dataSplit);
        AppendFileStoreWrite storeWrite = (AppendFileStoreWrite) store.newWrite(commitUser);
        storeWrite.withWriteType(table.rowType());
        RecordWriter<InternalRow> writer = storeWrite.createWriter(partition, 0);

        try {
            reader.forEachRemaining(
                    row -> {
                        try {
                            writer.write(row);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });

            compactAfter.addAll(writer.prepareCommit(false).newFilesIncrement().newFiles());
        } finally {
            writer.close();
            storeWrite.close();
        }

        return commitMessage(compactBefore, compactAfter);
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
