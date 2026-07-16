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
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FileWriter;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Range;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.types.BlobType.fieldNamesInBlobFile;
import static org.apache.paimon.utils.DataEvolutionUtils.checkContiguousRowRange;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compacts dedicated blob files of a data evolution table. */
public class DataEvolutionBlobCompactTask extends DataEvolutionCompactTask {

    public DataEvolutionBlobCompactTask(BinaryRow partition, List<DataFileMeta> files) {
        super(partition, files);
    }

    @Override
    public TaskType type() {
        return TaskType.BLOB;
    }

    @Override
    public CommitMessage doCompact(FileStoreTable table, String commitUser) throws Exception {
        CoreOptions options = table.coreOptions();
        List<DataFileMeta> sortedCompactBefore = sortedByFirstRowId(compactBefore);
        DataField blobField = blobField(table, options, sortedCompactBefore);
        Range compactBeforeRange = checkContiguousRowRange(sortedCompactBefore);
        checkArgument(
                sortedCompactBefore.size() > 1,
                "Blob compaction task %s should contain at least two files to compact.",
                this);

        RowType blobWriteType = new RowType(Collections.singletonList(blobField));

        FileStoreTable readTable = table.copy(BLOB_COMPACT_READ_OPTIONS);
        AppendOnlyFileStore store = (AppendOnlyFileStore) readTable.store();
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

        DataFileMeta compactedFile =
                writer.result()
                        .assignFirstRowId(compactBeforeRange.from)
                        .assignSequenceNumber(
                                minSequenceId(sortedCompactBefore),
                                maxSequenceId(sortedCompactBefore));
        compactAfter.add(compactedFile);
        checkArgument(compactAfter.size() == 1, "Blob file compaction should produce one file.");
        checkSameRowRange("Blob compact files", sortedCompactBefore, compactAfter);

        return commitMessage(sortedCompactBefore, compactAfter);
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

    private DataField blobField(
            FileStoreTable table, CoreOptions options, List<DataFileMeta> files) {
        Integer blobFieldId = null;
        Map<Long, RowType> schemaCache = new HashMap<>();
        for (DataFileMeta file : files) {
            checkArgument(
                    file.writeCols() != null && file.writeCols().size() == 1,
                    "Blob file %s should contain exactly one write column.",
                    file);
            RowType fileRowType =
                    schemaCache.computeIfAbsent(
                            file.schemaId(),
                            schemaId -> table.schemaManager().schema(schemaId).logicalRowType());
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
}
