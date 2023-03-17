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

package org.apache.paimon.file.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.file.append.AppendOnlyCompactManager;
import org.apache.paimon.file.append.AppendOnlyWriter;
import org.apache.paimon.file.compact.CompactManager;
import org.apache.paimon.file.compact.NoopCompactManager;
import org.apache.paimon.file.io.DataFileMeta;
import org.apache.paimon.file.io.DataFilePathFactory;
import org.apache.paimon.file.io.RowDataRollingFileWriter;
import org.apache.paimon.file.utils.CommitIncrement;
import org.apache.paimon.file.utils.FileStorePathFactory;
import org.apache.paimon.file.utils.RecordWriter;
import org.apache.paimon.file.utils.SnapshotManager;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.file.io.DataFileMeta.getMaxSequenceNumber;

/** {@link FileStoreWrite} for {@link org.apache.paimon.file.AppendOnlyFileStore}. */
public class AppendOnlyFileStoreWrite extends AbstractFileStoreWrite<InternalRow> {

    private final FileIO fileIO;
    private final AppendOnlyFileStoreRead read;
    private final long schemaId;
    private final RowType rowType;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    private final long targetFileSize;
    private final int compactionMinFileNum;
    private final int compactionMaxFileNum;
    private final boolean commitForceCompact;
    private final boolean skipCompaction;

    public AppendOnlyFileStoreWrite(
            FileIO fileIO,
            AppendOnlyFileStoreRead read,
            long schemaId,
            String commitUser,
            RowType rowType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options) {
        super(commitUser, snapshotManager, scan);
        this.fileIO = fileIO;
        this.read = read;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.fileFormat = options.fileFormat();
        this.pathFactory = pathFactory;
        this.targetFileSize = options.targetFileSize();
        this.compactionMinFileNum = options.compactionMinFileNum();
        this.compactionMaxFileNum = options.compactionMaxFileNum();
        this.commitForceCompact = options.commitForceCompact();
        this.skipCompaction = options.writeOnly();
    }

    @Override
    protected RecordWriter<InternalRow> createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor) {
        // let writer and compact manager hold the same reference
        // and make restore files mutable to update
        LinkedList<DataFileMeta> restored = new LinkedList<>(restoredFiles);
        DataFilePathFactory factory = pathFactory.createDataFilePathFactory(partition, bucket);
        CompactManager compactManager =
                skipCompaction
                        ? new NoopCompactManager()
                        : new AppendOnlyCompactManager(
                                fileIO,
                                compactExecutor,
                                restored,
                                compactionMinFileNum,
                                compactionMaxFileNum,
                                targetFileSize,
                                compactRewriter(partition, bucket),
                                factory);
        return new AppendOnlyWriter(
                fileIO,
                schemaId,
                fileFormat,
                targetFileSize,
                rowType,
                getMaxSequenceNumber(restored),
                compactManager,
                commitForceCompact,
                factory,
                restoreIncrement);
    }

    private AppendOnlyCompactManager.CompactRewriter compactRewriter(
            BinaryRow partition, int bucket) {
        return toCompact -> {
            if (toCompact.isEmpty()) {
                return Collections.emptyList();
            }
            RowDataRollingFileWriter rewriter =
                    new RowDataRollingFileWriter(
                            fileIO,
                            schemaId,
                            fileFormat,
                            targetFileSize,
                            rowType,
                            pathFactory.createDataFilePathFactory(partition, bucket),
                            new LongCounter(toCompact.get(0).minSequenceNumber()));
            rewriter.write(
                    new RecordReaderIterator<>(
                            read.createReader(
                                    new DataSplit(
                                            0L /* unused */,
                                            partition,
                                            bucket,
                                            toCompact,
                                            false))));
            rewriter.close();
            return rewriter.result();
        };
    }
}
