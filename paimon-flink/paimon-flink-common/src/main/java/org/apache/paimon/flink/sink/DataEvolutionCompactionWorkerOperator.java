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

import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.flink.source.DataEvolutionTableCompactSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Operator to execute {@link DataEvolutionCompactTask} passed from {@link
 * DataEvolutionTableCompactSource} for compacting single data-evolution table.
 */
public class DataEvolutionCompactionWorkerOperator
        extends PrepareCommitOperator<DataEvolutionCompactTask, Committable> {

    private final FileStoreTable fileStoreTable;
    private final String commitUser;
    private final List<Committable> committables;

    private DataEvolutionCompactionWorkerOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            String commitUser) {
        super(parameters, Options.fromMap(table.options()));
        this.fileStoreTable = table;
        this.commitUser = commitUser;
        this.committables = new ArrayList<>();
    }

    @Override
    public void processElement(StreamRecord<DataEvolutionCompactTask> element) throws Exception {
        DataEvolutionCompactTask task = element.getValue();
        committables.add(
                new Committable(Long.MAX_VALUE, task.doCompact(fileStoreTable, commitUser)));
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> toCommit = new ArrayList<>(committables);
        committables.clear();
        return toCommit;
    }

    /** {@link StreamOperatorFactory} of {@link DataEvolutionCompactionWorkerOperator}. */
    public static class Factory
            extends PrepareCommitOperator.Factory<DataEvolutionCompactTask, Committable> {

        private final FileStoreTable fileStoreTable;
        private final String commitUser;

        public Factory(FileStoreTable table, String initialCommitUser) {
            super(Options.fromMap(table.options()));
            this.fileStoreTable = table;
            this.commitUser = initialCommitUser;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            return (T)
                    new DataEvolutionCompactionWorkerOperator(
                            parameters, fileStoreTable, commitUser);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return DataEvolutionCompactionWorkerOperator.class;
        }
    }
}
