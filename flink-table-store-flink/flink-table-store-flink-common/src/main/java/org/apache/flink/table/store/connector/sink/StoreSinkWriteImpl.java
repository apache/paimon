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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.disk.IOManagerImpl;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.CommitMessage;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.store.table.sink.TableWriteImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Default implementation of {@link StoreSinkWrite}. This writer does not have states. */
public class StoreSinkWriteImpl implements StoreSinkWrite {

    private static final Logger LOG = LoggerFactory.getLogger(StoreSinkWriteImpl.class);

    protected final FileStoreTable table;
    protected final String commitUser;
    protected final TableWriteImpl<?> write;

    public StoreSinkWriteImpl(
            FileStoreTable table,
            StateInitializationContext context,
            String initialCommitUser,
            IOManager ioManager,
            boolean isOverwrite)
            throws Exception {
        this.table = table;

        // Each job can only have one user name and this name must be consistent across restarts.
        // We cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint.
        commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);

        // State will be null if the upstream of this subtask has finished, but some other subtasks
        // are still running.
        // See comments of StateUtils.getSingleValueFromState for more detail.
        //
        // If the state is null, no new records will come. We only need to deal with checkpoints and
        // close events.
        if (commitUser == null) {
            write = null;
        } else {
            write =
                    table.newWrite(commitUser)
                            .withIOManager(
                                    new IOManagerImpl(ioManager.getSpillingDirectoriesPaths()))
                            .withOverwrite(isOverwrite);
        }
    }

    @Override
    public SinkRecord write(InternalRow rowData) throws Exception {
        return write.writeAndReturn(rowData);
    }

    @Override
    public SinkRecord toLogRecord(SinkRecord record) {
        return write.toLogRecord(record);
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        write.compact(partition, bucket, fullCompaction);
    }

    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Receive {} new files from snapshot {}, partition {}, bucket {}",
                    files.size(),
                    snapshotId,
                    partition,
                    bucket);
        }
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    @Override
    public List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = new ArrayList<>();
        if (write != null) {
            try {
                for (CommitMessage committable : write.prepareCommit(doCompaction, checkpointId)) {
                    committables.add(
                            new Committable(checkpointId, Committable.Kind.FILE, committable));
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return committables;
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // do nothing
    }

    @Override
    public void close() throws Exception {
        if (write != null) {
            write.close();
        }
    }
}
