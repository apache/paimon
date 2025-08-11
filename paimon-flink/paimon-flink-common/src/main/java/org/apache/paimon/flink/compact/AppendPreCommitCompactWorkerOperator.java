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

package org.apache.paimon.flink.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.FileStorePathFactory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Receive and process the {@link AppendCompactTask}s emitted by {@link
 * AppendPreCommitCompactCoordinatorOperator}.
 */
public class AppendPreCommitCompactWorkerOperator extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<
                Either<Committable, Tuple2<Long, AppendCompactTask>>, Committable> {

    private final FileStoreTable table;

    private transient AppendFileStoreWrite write;
    private transient FileStorePathFactory pathFactory;
    private transient FileIO fileIO;

    public AppendPreCommitCompactWorkerOperator(FileStoreTable table) {
        this.table = table;
    }

    @Override
    public void open() throws Exception {
        super.open();
        CoreOptions coreOptions = new CoreOptions(table.options());
        this.write = (AppendFileStoreWrite) table.store().newWrite(null);
        if (coreOptions.rowTrackingEnabled()) {
            checkArgument(
                    !coreOptions.dataEvolutionEnabled(),
                    "Data evolution enabled table should not invoke compact yet.");
            this.write.withWriteType(SpecialFields.rowTypeWithRowLineage(table.rowType()));
        }
        this.pathFactory = table.store().pathFactory();
        this.fileIO = table.fileIO();
    }

    @Override
    public void processElement(
            StreamRecord<Either<Committable, Tuple2<Long, AppendCompactTask>>> record)
            throws Exception {
        if (record.getValue().isLeft()) {
            output.collect(new StreamRecord<>(record.getValue().left()));
        } else {
            long checkpointId = record.getValue().right().f0;
            CommitMessage message = doCompact(record.getValue().right().f1);
            output.collect(
                    new StreamRecord<>(
                            new Committable(checkpointId, Committable.Kind.FILE, message)));
        }
    }

    private CommitMessage doCompact(AppendCompactTask task) throws Exception {
        CommitMessageImpl message = (CommitMessageImpl) task.doCompact(table, write);

        Map<String, DataFileMeta> toDelete = new HashMap<>();
        for (DataFileMeta meta : message.compactIncrement().compactBefore()) {
            toDelete.put(meta.fileName(), meta);
        }
        for (DataFileMeta meta : message.compactIncrement().compactAfter()) {
            toDelete.remove(meta.fileName());
        }
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(task.partition(), message.bucket());
        for (DataFileMeta meta : toDelete.values()) {
            fileIO.deleteQuietly(dataFilePathFactory.toPath(meta));
        }

        return new CommitMessageImpl(
                message.partition(),
                message.bucket(),
                message.totalBuckets(),
                new DataIncrement(
                        message.compactIncrement().compactAfter(),
                        Collections.emptyList(),
                        Collections.emptyList()),
                CompactIncrement.emptyIncrement());
    }

    @Override
    public void close() throws Exception {
        if (write != null) {
            write.close();
        }
    }
}
