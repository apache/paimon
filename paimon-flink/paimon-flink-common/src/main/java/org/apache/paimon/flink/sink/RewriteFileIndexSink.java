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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.procedure.RewriteFileIndexProcedure;
import org.apache.paimon.index.FileIndexProcessor;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** File index sink for {@link RewriteFileIndexProcedure}. */
public class RewriteFileIndexSink extends FlinkWriteSink<ManifestEntry> {

    public RewriteFileIndexSink(FileStoreTable table) {
        super(table, null);
    }

    @Override
    protected OneInputStreamOperatorFactory<ManifestEntry, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new FileIndexModificationOperatorFactory(
                table.coreOptions().toConfiguration(), table);
    }

    private static class FileIndexModificationOperatorFactory
            extends PrepareCommitOperator.Factory<ManifestEntry, Committable> {
        private final FileStoreTable table;

        public FileIndexModificationOperatorFactory(Options options, FileStoreTable table) {
            super(options);
            this.table = table;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            return (T) new FileIndexModificationOperator(parameters, options, table);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return FileIndexModificationOperator.class;
        }
    }

    /** File index modification operator to rewrite file index. */
    private static class FileIndexModificationOperator
            extends PrepareCommitOperator<ManifestEntry, Committable> {

        private static final long serialVersionUID = 1L;

        private final transient FileIndexProcessor fileIndexProcessor;
        private final transient List<CommitMessage> messages;

        private FileIndexModificationOperator(
                StreamOperatorParameters<Committable> parameters,
                Options options,
                FileStoreTable table) {
            super(parameters, options);
            this.fileIndexProcessor = new FileIndexProcessor(table);
            this.messages = new ArrayList<>();
        }

        @Override
        public void processElement(StreamRecord<ManifestEntry> element) throws Exception {
            ManifestEntry entry = element.getValue();
            BinaryRow partition = entry.partition();
            int bucket = entry.bucket();
            DataFileMeta indexedFile = fileIndexProcessor.process(partition, bucket, entry);

            CommitMessageImpl commitMessage =
                    new CommitMessageImpl(
                            partition,
                            bucket,
                            entry.totalBuckets(),
                            DataIncrement.emptyIncrement(),
                            new CompactIncrement(
                                    Collections.singletonList(entry.file()),
                                    Collections.singletonList(indexedFile),
                                    Collections.emptyList()));

            messages.add(commitMessage);
        }

        @Override
        protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId) {
            ArrayList<CommitMessage> temp = new ArrayList<>(messages);
            messages.clear();
            return temp.stream()
                    .map(s -> new Committable(checkpointId, s))
                    .collect(Collectors.toList());
        }
    }
}
