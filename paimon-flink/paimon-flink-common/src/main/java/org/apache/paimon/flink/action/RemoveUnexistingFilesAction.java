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

package org.apache.paimon.flink.action;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.operation.ListUnexistingFiles;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Action to remove unexisting data files from manifest entries. It has the following use cases:
 *
 * <ul>
 *   <li>There is currently a known case when unexisting data file might be written into manifest.
 *       Consider a write-only job (W) plus a dedicated compaction job (C):
 *       <ol>
 *         <li>W commits a snapshot with file F. Then W constantly fails and restarts, each time
 *             before we can retry the commit (or W is stopped, creating Flink savepoint S).
 *         <li>C compacts F into a larger file, so F is now deleted from the manifest.
 *         <li>Before the compact snapshot expires, but after all snapshots created by W expires, W
 *             comes back to normal (or restarted from savepoint S). As W cannot find its previous
 *             snapshot, it assumes that this snapshot has not been committed (see {@link
 *             org.apache.paimon.operation.FileStoreCommitImpl#filterCommitted} for more detail), so
 *             file F is committed to the manifest once again.
 *         <li>When the compact snapshot expires, file F will be deleted from the file system. Now F
 *             is in the manifest, but not on the file system. With this situation, user might want
 *             to remove F from the manifest to continue reading the table.
 *       </ol>
 *   <li>User deletes a data file by mistake (for example, by incorrectly setting the time threshold
 *       for orphan files cleaning). If the user can tolerate skipping some records when consuming
 *       this table, he can also use this action to remove the file from manifest.
 * </ul>
 *
 * <p>Note that user is on his own risk using this procedure, which may cause data loss when used
 * outside from the use cases above.
 */
public class RemoveUnexistingFilesAction extends TableActionBase {

    private static final OutputTag<String> RESULT_SIDE_OUTPUT =
            new OutputTag<>("result-side-output", BasicTypeInfo.STRING_TYPE_INFO);

    private boolean dryRun = false;
    @Nullable private Integer parallelism = null;

    public RemoveUnexistingFilesAction(
            String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
    }

    public RemoveUnexistingFilesAction dryRun() {
        this.dryRun = true;
        return this;
    }

    public RemoveUnexistingFilesAction withParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    @Override
    public void build() throws Exception {
        buildDataStream();
    }

    public DataStream<String> buildDataStream() throws Exception {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<BinaryRow> binaryPartitions = fileStoreTable.newScan().listPartitions();

        SingleOutputStreamOperator<byte[]> source =
                env.fromData(
                                binaryPartitions.stream()
                                        .map(BinaryRow::toBytes)
                                        .collect(Collectors.toList()),
                                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                        .name("Remove Unexisting Files Source")
                        .forceNonParallel();

        SingleOutputStreamOperator<Committable> worker =
                source.transform(
                        "Remove Unexisting Files Worker",
                        new CommittableTypeInfo(),
                        new WorkerOperator(fileStoreTable));
        if (parallelism != null) {
            worker = worker.setParallelism(Math.min(parallelism, binaryPartitions.size()));
        }

        DataStream<String> result = worker.getSideOutput(RESULT_SIDE_OUTPUT);
        if (dryRun) {
            return result;
        }

        worker.transform(
                        "Global Committer : " + table.name(),
                        new CommittableTypeInfo(),
                        new CommitOperator(fileStoreTable))
                .forceNonParallel();
        return result;
    }

    @Override
    public void run() throws Exception {
        build();
        env.execute("Remove Unexisting Files : " + table.name());
    }

    private static class WorkerOperator extends BoundedOneInputOperator<byte[], Committable> {

        private static final long serialVersionUID = 1L;

        private final FileStoreTable table;

        private transient ListUnexistingFiles operation;
        private transient BinaryRow reuse;

        private WorkerOperator(FileStoreTable table) {
            this.table = table;
        }

        @Override
        public void open() throws Exception {
            operation = new ListUnexistingFiles(table);
            reuse = new BinaryRow(table.schema().partitionKeys().size());
        }

        @Override
        public void processElement(StreamRecord<byte[]> record) throws Exception {
            byte[] bytes = record.getValue();
            reuse.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
            Map<Integer, Map<String, DataFileMeta>> toDelete = operation.list(reuse);
            for (Map.Entry<Integer, Map<String, DataFileMeta>> entry : toDelete.entrySet()) {
                CommitMessageImpl message =
                        new CommitMessageImpl(
                                reuse,
                                entry.getKey(),
                                new DataIncrement(
                                        Collections.emptyList(),
                                        new ArrayList<>(entry.getValue().values()),
                                        Collections.emptyList()),
                                CompactIncrement.emptyIncrement());
                output.collect(
                        new StreamRecord<>(
                                new Committable(Long.MAX_VALUE, Committable.Kind.FILE, message)));
                for (String path : entry.getValue().keySet()) {
                    output.collect(RESULT_SIDE_OUTPUT, new StreamRecord<>(path));
                }
            }
        }

        @Override
        public void endInput() throws Exception {}
    }

    private static class CommitOperator extends BoundedOneInputOperator<Committable, Committable> {

        private static final long serialVersionUID = 1L;

        private final FileStoreTable table;

        private transient List<CommitMessage> commitMessages;
        private transient TableCommitImpl commit;

        private CommitOperator(FileStoreTable table) {
            this.table = table;
        }

        @Override
        public void open() throws Exception {
            commitMessages = new ArrayList<>();
            commit = table.newCommit(UUID.randomUUID().toString());
        }

        @Override
        public void processElement(StreamRecord<Committable> record) throws Exception {
            Committable committable = record.getValue();
            Preconditions.checkArgument(
                    committable.kind() == Committable.Kind.FILE,
                    "Committable has kind " + committable.kind() + ". This is unexpected!");
            commitMessages.add((CommitMessage) committable.wrappedCommittable());
        }

        @Override
        public void endInput() throws Exception {
            try {
                commit.commit(Long.MAX_VALUE, commitMessages);
            } catch (Exception e) {
                // For batch jobs we don't know if this commit is new or being
                // retried, so in theory we need to call filterAndCommit.
                //
                // However on the happy path, filtering takes time because there
                // is no previous commit of this user, and the filtering process
                // must go through all existing snapshots to determine this.
                //
                // So instead, we ask the user to retry this job if the commit
                // failed, most probably due to a conflict. Why not throw this
                // exception? Because throwing the exception will restart the
                // job, if it is a batch job we'll have to filter the commit.
                //
                // Retrying this job will calculate what file entries to remove
                // again, so there is no harm.
                LOG.warn(
                        "Commit failed due to exception. "
                                + "Consider running this action or procedure again.",
                        e);
            }
        }

        @Override
        public void close() throws Exception {
            if (commit != null) {
                commit.close();
            }
        }
    }
}
