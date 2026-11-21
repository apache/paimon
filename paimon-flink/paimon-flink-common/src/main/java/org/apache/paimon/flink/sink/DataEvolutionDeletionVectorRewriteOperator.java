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

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionDeletionVectorRewritePlanner;
import org.apache.paimon.append.dataevolution.DataEvolutionDeletionVectorRewriteTask;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Executes partition-local DV batches in parallel, emitting only commit metadata. */
public class DataEvolutionDeletionVectorRewriteOperator extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<byte[], Committable> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DataEvolutionDeletionVectorRewriteOperator.class);

    static final OutputTag<Committable> DATA_COMMITS =
            new OutputTag<>("data-evolution-data-commits", new CommittableTypeInfo());

    private final FileStoreTable table;
    private final DataEvolutionDeletionVectorRewriteTask.Serializer serializer =
            new DataEvolutionDeletionVectorRewriteTask.Serializer();
    private final List<Path> outputs = new ArrayList<>();
    private boolean finished;

    private DataEvolutionDeletionVectorRewriteOperator(
            StreamOperatorParameters<Committable> parameters, FileStoreTable table) {
        this.table = table;
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public void processElement(StreamRecord<byte[]> element) throws Exception {
        try {
            DataEvolutionDeletionVectorRewriteTask task =
                    serializer.deserialize(serializer.getVersion(), element.getValue());
            CommitMessageImpl message = (CommitMessageImpl) task.execute(table);
            IndexPathFactory paths =
                    table.store()
                            .pathFactory()
                            .indexFileFactory(message.partition(), message.bucket());
            for (IndexFileMeta file : message.compactIncrement().newIndexFiles()) {
                outputs.add(paths.toPath(file));
            }
            output.collect(new StreamRecord<>(new Committable(Long.MAX_VALUE, message)));
        } catch (Exception failure) {
            cleanup();
            throw failure;
        }
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        finished = true;
        outputs.clear();
    }

    @Override
    public void close() throws Exception {
        try {
            if (!finished) {
                cleanup();
            }
        } finally {
            super.close();
        }
    }

    private void cleanup() {
        for (Path output : outputs) {
            try {
                table.fileIO().deleteQuietly(output);
            } catch (RuntimeException failure) {
                LOG.warn("Failed to delete uncommitted DV index file {}.", output, failure);
            }
        }
        outputs.clear();
    }

    /** Collects all data outputs before assigning each old DV index file to exactly one task. */
    private static class Planner extends PrepareCommitOperator<Committable, byte[]> {

        private final FileStoreTable table;
        private final Snapshot snapshot;
        private final List<Committable> committables = new ArrayList<>();

        private Planner(
                StreamOperatorParameters<byte[]> parameters,
                FileStoreTable table,
                Snapshot snapshot) {
            super(parameters, Options.fromMap(table.options()));
            this.table = table;
            this.snapshot = snapshot;
        }

        @Override
        public void processElement(StreamRecord<Committable> element) {
            committables.add(element.getValue());
        }

        @Override
        protected List<byte[]> prepareCommit(boolean waitCompaction, long checkpointId)
                throws IOException {
            List<CommitMessage> messages = new ArrayList<>(committables.size());
            for (Committable committable : committables) {
                messages.add(committable.commitMessage());
                output.collect(DATA_COMMITS, new StreamRecord<>(committable));
            }
            committables.clear();
            List<byte[]> tasks = new ArrayList<>();
            DataEvolutionDeletionVectorRewriteTask.Serializer serializer =
                    new DataEvolutionDeletionVectorRewriteTask.Serializer();
            for (DataEvolutionDeletionVectorRewriteTask task :
                    new DataEvolutionDeletionVectorRewritePlanner(table, snapshot).plan(messages)) {
                tasks.add(serializer.serialize(task));
            }
            return tasks;
        }
    }

    /** Factory for the metadata-only planner. */
    public static class PlannerFactory extends PrepareCommitOperator.Factory<Committable, byte[]> {
        private final FileStoreTable table;
        private final Snapshot snapshot;

        public PlannerFactory(FileStoreTable table, Snapshot snapshot) {
            super(Options.fromMap(table.options()));
            this.table = table;
            this.snapshot = snapshot;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<byte[]>> T createStreamOperator(
                StreamOperatorParameters<byte[]> parameters) {
            return (T) new Planner(parameters, table, snapshot);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return Planner.class;
        }
    }

    /** Factory for the parallel bitmap workers. */
    public static class Factory extends AbstractStreamOperatorFactory<Committable>
            implements OneInputStreamOperatorFactory<byte[], Committable> {
        private final FileStoreTable table;

        public Factory(FileStoreTable table) {
            this.table = table;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            return (T) new DataEvolutionDeletionVectorRewriteOperator(parameters, table);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return DataEvolutionDeletionVectorRewriteOperator.class;
        }
    }
}
