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

package org.apache.paimon.flink.sink;

import org.apache.paimon.globalindex.btree.BTreeGlobalIndexCompactTask;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexCompactWorker;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Operator to execute {@link BTreeGlobalIndexCompactTask} for distributed BTree global index
 * compaction.
 *
 * <p>Each task is executed by a {@link BTreeGlobalIndexCompactWorker}. The results (DELETE old
 * entries + ADD new entries) are converted to {@link CommitMessage} via {@link CompactIncrement}'s
 * index file lists.
 */
public class CompactIndexWorkerOperator
        extends PrepareCommitOperator<BTreeGlobalIndexCompactTask, Committable> {

    private final FileStoreTable table;
    private final String commitUser;
    private final List<Committable> committables;

    private transient BTreeGlobalIndexCompactWorker worker;

    private CompactIndexWorkerOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            String commitUser) {
        super(parameters, Options.fromMap(table.options()));
        this.table = table;
        this.commitUser = commitUser;
        this.committables = new ArrayList<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        Options options = table.coreOptions().toConfiguration();
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        this.worker =
                new BTreeGlobalIndexCompactWorker(
                        table.fileIO(), table.rowType(), options, indexPathFactory);
    }

    @Override
    public void processElement(StreamRecord<BTreeGlobalIndexCompactTask> element) throws Exception {
        BTreeGlobalIndexCompactTask task = element.getValue();

        // Execute the compaction task
        List<IndexManifestEntry> newEntries = worker.execute(task);

        // Build CommitMessage: DELETE source entries + ADD new entries
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        for (IndexManifestEntry sourceEntry : task.sourceEntries()) {
            deletedIndexFiles.add(sourceEntry.indexFile());
        }

        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        for (IndexManifestEntry newEntry : newEntries) {
            newIndexFiles.add(newEntry.indexFile());
        }

        CompactIncrement compactIncrement =
                new CompactIncrement(
                        Collections.<org.apache.paimon.io.DataFileMeta>emptyList(),
                        Collections.<org.apache.paimon.io.DataFileMeta>emptyList(),
                        Collections.<org.apache.paimon.io.DataFileMeta>emptyList(),
                        newIndexFiles,
                        deletedIndexFiles);

        CommitMessage commitMessage =
                new CommitMessageImpl(
                        task.partition(),
                        task.bucket(),
                        null,
                        DataIncrement.emptyIncrement(),
                        compactIncrement);

        committables.add(new Committable(Long.MAX_VALUE, commitMessage));
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> toCommit = new ArrayList<>(committables);
        committables.clear();
        return toCommit;
    }

    /** {@link StreamOperatorFactory} of {@link CompactIndexWorkerOperator}. */
    public static class Factory
            extends PrepareCommitOperator.Factory<BTreeGlobalIndexCompactTask, Committable> {

        private final FileStoreTable table;
        private final String commitUser;

        public Factory(FileStoreTable table, String commitUser) {
            super(Options.fromMap(table.options()));
            this.table = table;
            this.commitUser = commitUser;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            return (T) new CompactIndexWorkerOperator(parameters, table, commitUser);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return CompactIndexWorkerOperator.class;
        }
    }
}
