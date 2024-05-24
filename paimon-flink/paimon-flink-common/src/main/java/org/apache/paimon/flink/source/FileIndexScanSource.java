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

package org.apache.paimon.flink.source;

import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Bounded {@link FlinkSource} for reading records. It does not monitor new snapshots. */
public class FileIndexScanSource
        implements Source<
                ManifestEntry, FileIndexScanSource.Split, FileIndexScanSource.CheckpointState> {

    private static final long serialVersionUID = 2319102734891237489L;

    private final FileStoreTable table;
    @Nullable private final Predicate partitionPredicate;

    public FileIndexScanSource(FileStoreTable table, @Nullable Predicate partitionPredicate) {
        this.table = table;
        this.partitionPredicate = partitionPredicate;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<Split, CheckpointState> createEnumerator(
            SplitEnumeratorContext<Split> splitEnumeratorContext) throws Exception {
        List<ManifestEntry> manifestEntries =
                table.store().newScan().withPartitionFilter(partitionPredicate).plan().files();
        return new ManifestFileSplitEnumerator(
                splitEnumeratorContext,
                manifestEntries.stream().map(Split::new).collect(Collectors.toList()));
    }

    @Override
    public SplitEnumerator<Split, CheckpointState> restoreEnumerator(
            SplitEnumeratorContext<Split> splitEnumeratorContext, CheckpointState checkpointState)
            throws Exception {
        return new ManifestFileSplitEnumerator(splitEnumeratorContext, checkpointState.files());
    }

    @Override
    public SimpleVersionedSerializer<Split> getSplitSerializer() {
        return new SplitSerder();
    }

    @Override
    public SimpleVersionedSerializer<CheckpointState> getEnumeratorCheckpointSerializer() {
        return new CheckpointSerde();
    }

    @Override
    public SourceReader<ManifestEntry, Split> createReader(SourceReaderContext sourceReaderContext)
            throws Exception {
        return new Reader(sourceReaderContext);
    }

    /** State for splits. */
    public static class CheckpointState {

        private final List<Split> files;

        public CheckpointState(List<Split> files) {
            this.files = files;
        }

        public List<Split> files() {
            return files;
        }
    }

    /** Enumerator to generate splits. */
    private static class ManifestFileSplitEnumerator
            implements SplitEnumerator<Split, CheckpointState> {

        private final SplitEnumeratorContext<Split> splitEnumeratorContext;
        private final List<Split> files;

        public ManifestFileSplitEnumerator(
                SplitEnumeratorContext<Split> splitEnumeratorContext, List<Split> files) {
            this.splitEnumeratorContext = splitEnumeratorContext;
            this.files = files;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int i, @Nullable String s) {
            if (!files.isEmpty()) {
                splitEnumeratorContext.assignSplit(files.remove(0), i);
            } else {
                splitEnumeratorContext.signalNoMoreSplits(i);
            }
        }

        @Override
        public void addSplitsBack(List<Split> list, int i) {
            files.addAll(list);
        }

        @Override
        public void addReader(int i) {}

        @Override
        public CheckpointState snapshotState(long l) throws Exception {
            return new CheckpointState(files);
        }

        @Override
        public void close() throws IOException {}
    }

    /** Split to wrap ManifestEntry. */
    public static class Split implements SourceSplit {

        private final ManifestEntry manifestEntry;

        public Split(ManifestEntry manifestEntry) {
            this.manifestEntry = manifestEntry;
        }

        @Override
        public String splitId() {
            return "splitId";
        }

        ManifestEntry entry() {
            return manifestEntry;
        }
    }

    private static class SplitSerder implements SimpleVersionedSerializer<Split> {

        private static final ManifestEntrySerializer manifestEntrySerializer =
                new ManifestEntrySerializer();

        public SplitSerder() {}

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Split sourceSplit) throws IOException {
            return manifestEntrySerializer.serializeToBytes(sourceSplit.entry());
        }

        @Override
        public Split deserialize(int i, byte[] bytes) throws IOException {
            return new Split(manifestEntrySerializer.deserializeFromBytes(bytes));
        }
    }

    private static class CheckpointSerde implements SimpleVersionedSerializer<CheckpointState> {

        private final SplitSerder splitSerder = new SplitSerder();

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(CheckpointState checkpointState) throws IOException {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutput dataOutput = new DataOutputStream(byteArrayOutputStream);
            List<Split> files = checkpointState.files();
            dataOutput.writeInt(files.size());
            for (Split file : files) {
                byte[] b = splitSerder.serialize(file);
                dataOutput.writeInt(b.length);
                dataOutput.write(b);
            }
            return new byte[0];
        }

        @Override
        public CheckpointState deserialize(int i, byte[] bytes) throws IOException {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInput dataInput = new DataInputStream(byteArrayInputStream);
            int size = dataInput.readInt();
            List<Split> files = new ArrayList<>();
            for (int j = 0; j < size; j++) {
                byte[] b = new byte[dataInput.readInt()];
                dataInput.readFully(b);
                files.add(splitSerder.deserialize(0, b));
            }
            return new CheckpointState(files);
        }
    }

    /** Reader for data metafile split. */
    private static class Reader implements SourceReader<ManifestEntry, Split> {

        private final SourceReaderContext context;
        private final ArrayDeque<Split> splits;

        private boolean noMore;

        public Reader(SourceReaderContext sourceReaderContext) {
            this.context = sourceReaderContext;
            this.splits = new ArrayDeque<>();
        }

        @Override
        public void start() {
            context.sendSplitRequest();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<ManifestEntry> readerOutput) throws Exception {
            if (!splits.isEmpty()) {
                readerOutput.collect(splits.poll().entry());
                if (!noMore && splits.isEmpty()) {
                    context.sendSplitRequest();
                }
                if (!splits.isEmpty()) {
                    return InputStatus.MORE_AVAILABLE;
                }
            }
            return noMore ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public List<Split> snapshotState(long l) {
            return new ArrayList<>(splits);
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return splits.isEmpty()
                    ? CompletableFuture.completedFuture(null)
                    : FutureCompletingBlockingQueue.AVAILABLE;
        }

        @Override
        public void addSplits(List<Split> list) {
            splits.addAll(list);
        }

        @Override
        public void notifyNoMoreSplits() {
            noMore = true;
        }

        @Override
        public void close() throws Exception {}
    }
}
