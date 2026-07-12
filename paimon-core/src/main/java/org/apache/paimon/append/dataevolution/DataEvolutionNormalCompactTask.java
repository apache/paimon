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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.types.BlobType.fieldNamesInBlobFile;
import static org.apache.paimon.types.VectorType.fieldNamesInVectorFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compacts normal structured files of a data evolution table. */
public class DataEvolutionNormalCompactTask extends DataEvolutionCompactTask {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionNormalCompactTask.class);

    public DataEvolutionNormalCompactTask(BinaryRow partition, List<DataFileMeta> files) {
        super(partition, files);
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
        dataFileMeta =
                dataFileMeta.assignSequenceNumber(
                        minSequenceId(compactBefore), maxSequenceId(compactBefore));
        compactAfter.add(dataFileMeta);

        return commitMessage(compactBefore, compactAfter);
    }
}
