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

import org.apache.paimon.append.dataevolution.DataEvolutionCompactionCommitPreparation;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Operator to rewrite deletion vectors for data-evolution compaction before committing. */
public class DataEvolutionCompactDeletionVectorOperator
        extends PrepareCommitOperator<Committable, Committable> {

    private final FileStoreTable table;
    private final List<Committable> committables;

    private DataEvolutionCompactDeletionVectorOperator(
            StreamOperatorParameters<Committable> parameters, FileStoreTable table) {
        super(parameters, Options.fromMap(table.options()));
        this.table = table;
        this.committables = new ArrayList<>();
    }

    @Override
    public void processElement(StreamRecord<Committable> element) {
        committables.add(element.getValue());
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> toCommit = new ArrayList<>(committables);
        committables.clear();
        if (toCommit.isEmpty()) {
            return toCommit;
        }

        List<CommitMessage> messages = new ArrayList<>(toCommit.size());
        for (Committable committable : toCommit) {
            messages.add(committable.commitMessage());
        }
        for (CommitMessage message :
                new DataEvolutionCompactionCommitPreparation(table).prepare(messages)) {
            toCommit.add(new Committable(toCommit.get(0).checkpointId(), message));
        }
        return toCommit;
    }

    /** {@link StreamOperatorFactory} of {@link DataEvolutionCompactDeletionVectorOperator}. */
    public static class Factory extends PrepareCommitOperator.Factory<Committable, Committable> {

        private final FileStoreTable table;

        public Factory(FileStoreTable table) {
            super(Options.fromMap(table.options()));
            this.table = table;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            return (T) new DataEvolutionCompactDeletionVectorOperator(parameters, table);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return DataEvolutionCompactDeletionVectorOperator.class;
        }
    }
}
