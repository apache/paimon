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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.format.FileFormat.fileFormat;

/** File writer for format table. */
public class FormatTableFileWriter {

    private final FileIO fileIO;
    private RowType rowType;
    private RowType partitionType;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    protected final Map<BinaryRow, FormatTableRecordWriter> writers;
    protected final CoreOptions options;

    public FormatTableFileWriter(
            FileIO fileIO, RowType rowType, CoreOptions options, RowType partitionType) {
        this.fileIO = fileIO;
        this.rowType = rowType;
        this.partitionType = partitionType;
        this.fileFormat = fileFormat(options);
        this.writers = new HashMap<>();
        this.options = options;
        this.pathFactory =
                new FileStorePathFactory(
                        options.path(),
                        partitionType,
                        options.partitionDefaultName(),
                        options.fileFormatString(),
                        options.dataFilePrefix(),
                        options.changelogFilePrefix(),
                        options.legacyPartitionName(),
                        options.fileSuffixIncludeCompression(),
                        options.formatTableFileImplementation(),
                        options.dataFilePathDirectory(),
                        null,
                        false);
    }

    public void withWriteType(RowType writeType) {
        this.rowType = writeType;
    }

    public void write(BinaryRow partition, InternalRow data) throws Exception {
        FormatTableRecordWriter writer = writers.get(partition);
        if (writer == null) {
            writer = createWriter(partition.copy());
            writers.put(partition.copy(), writer);
        }
        writer.write(data);
    }

    public void close() throws Exception {
        writers.clear();
    }

    public List<CommitMessage> prepareCommit() throws Exception {
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (FormatTableRecordWriter writer : writers.values()) {
            List<TwoPhaseOutputStream.Committer> commiters = writer.closeAndGetCommitters();
            for (TwoPhaseOutputStream.Committer committer : commiters) {
                TwoPhaseCommitMessage twoPhaseCommitMessage = new TwoPhaseCommitMessage(committer);
                commitMessages.add(twoPhaseCommitMessage);
            }
        }
        return commitMessages;
    }

    private FormatTableRecordWriter createWriter(BinaryRow partition) {
        RowType writeRowType =
                rowType.project(
                        rowType.getFieldNames().stream()
                                .filter(name -> !partitionType.getFieldNames().contains(name))
                                .collect(Collectors.toList()));
        return new FormatTableRecordWriter(
                fileIO,
                fileFormat,
                options.targetFileSize(false),
                pathFactory.createFormatTableDataFilePathFactory(
                        partition, options.formatTablePartitionOnlyValueInPath()),
                writeRowType,
                options.formatTableFileImplementation());
    }
}
