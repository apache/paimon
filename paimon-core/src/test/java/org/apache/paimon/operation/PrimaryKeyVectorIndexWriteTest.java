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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pkvector.BucketedVectorIndexMaintainer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests primary-key vector index publication through {@link AbstractFileStoreWrite}. */
class PrimaryKeyVectorIndexWriteTest {

    @Test
    void testRestoreRebindsVectorIndexExecutor() throws Exception {
        DataIncrement dataIncrement = DataIncrement.emptyIncrement();
        CompactIncrement compactIncrement = CompactIncrement.emptyIncrement();
        RecordWriter<String> writer = mock(RecordWriter.class);
        when(writer.prepareCommit(false))
                .thenReturn(new CommitIncrement(dataIncrement, compactIncrement, null));

        BucketedVectorIndexMaintainer.VectorIndexCommit vectorCommit =
                mock(BucketedVectorIndexMaintainer.VectorIndexCommit.class);
        when(vectorCommit.appendIncrement()).thenReturn(Optional.empty());
        when(vectorCommit.compactIncrement()).thenReturn(Optional.empty());
        BucketedVectorIndexMaintainer maintainer = mock(BucketedVectorIndexMaintainer.class);
        when(maintainer.prepareCommit(same(dataIncrement), same(compactIncrement), eq(true)))
                .thenReturn(vectorCommit);

        TestingFileStoreWrite write = new TestingFileStoreWrite();
        write.install(writer, maintainer);
        List<FileStoreWrite.State<String>> states = write.checkpoint();

        TestingFileStoreWrite restored = new TestingFileStoreWrite(mock(RecordWriter.class));
        restored.restore(states);

        verify(maintainer).withExecutor(any(ExecutorService.class));
    }

    @Test
    void testCheckpointWaitsForPendingVectorIndexBuild() throws Exception {
        DataIncrement dataIncrement = DataIncrement.emptyIncrement();
        CompactIncrement compactIncrement = CompactIncrement.emptyIncrement();
        RecordWriter<String> writer = mock(RecordWriter.class);
        when(writer.prepareCommit(false))
                .thenReturn(new CommitIncrement(dataIncrement, compactIncrement, null));

        BucketedVectorIndexMaintainer.VectorIndexCommit vectorCommit =
                mock(BucketedVectorIndexMaintainer.VectorIndexCommit.class);
        when(vectorCommit.appendIncrement()).thenReturn(Optional.empty());
        when(vectorCommit.compactIncrement()).thenReturn(Optional.empty());
        BucketedVectorIndexMaintainer maintainer = mock(BucketedVectorIndexMaintainer.class);
        when(maintainer.prepareCommit(same(dataIncrement), same(compactIncrement), eq(true)))
                .thenReturn(vectorCommit);

        TestingFileStoreWrite write = new TestingFileStoreWrite();
        write.install(writer, maintainer);

        write.checkpoint();

        verify(maintainer).prepareCommit(same(dataIncrement), same(compactIncrement), eq(true));
    }

    @Test
    void testPublishesVectorChangesWithCompactDataTransition() throws Exception {
        DataIncrement dataIncrement = DataIncrement.emptyIncrement();
        CompactIncrement compactIncrement = CompactIncrement.emptyIncrement();
        RecordWriter<String> writer = mock(RecordWriter.class);
        when(writer.prepareCommit(false))
                .thenReturn(new CommitIncrement(dataIncrement, compactIncrement, null));

        IndexFileMeta added = mock(IndexFileMeta.class);
        IndexFileMeta deleted = mock(IndexFileMeta.class);
        BucketedVectorIndexMaintainer.VectorIndexIncrement vectorIncrement =
                mock(BucketedVectorIndexMaintainer.VectorIndexIncrement.class);
        when(vectorIncrement.newIndexFiles()).thenReturn(Collections.singletonList(added));
        when(vectorIncrement.deletedIndexFiles()).thenReturn(Collections.singletonList(deleted));
        BucketedVectorIndexMaintainer.VectorIndexCommit vectorCommit =
                mock(BucketedVectorIndexMaintainer.VectorIndexCommit.class);
        when(vectorCommit.appendIncrement()).thenReturn(Optional.empty());
        when(vectorCommit.compactIncrement()).thenReturn(Optional.of(vectorIncrement));
        BucketedVectorIndexMaintainer maintainer = mock(BucketedVectorIndexMaintainer.class);
        when(maintainer.prepareCommit(same(dataIncrement), same(compactIncrement), eq(false)))
                .thenReturn(vectorCommit);

        TestingFileStoreWrite write = new TestingFileStoreWrite();
        write.install(writer, maintainer);

        CommitMessageImpl message = (CommitMessageImpl) write.prepareCommit(false, 1).get(0);

        assertThat(message.compactIncrement().newIndexFiles()).containsExactly(added);
        assertThat(message.compactIncrement().deletedIndexFiles()).containsExactly(deleted);
        verify(maintainer).prepareCommit(same(dataIncrement), same(compactIncrement), eq(false));
    }

    private static class TestingFileStoreWrite extends AbstractFileStoreWrite<String> {

        @Nullable private final RecordWriter<String> restoredWriter;

        private TestingFileStoreWrite() {
            this(null);
        }

        private TestingFileStoreWrite(@Nullable RecordWriter<String> restoredWriter) {
            super(
                    mock(SnapshotManager.class),
                    mock(FileStoreScan.class),
                    null,
                    null,
                    null,
                    "test-table",
                    new CoreOptions(new HashMap<>()),
                    RowType.of());
            this.restoredWriter = restoredWriter;
        }

        private void install(
                RecordWriter<String> writer, BucketedVectorIndexMaintainer vectorMaintainer) {
            writers.computeIfAbsent(EMPTY_ROW, ignored -> new HashMap<>())
                    .put(0, new WriterContainer<>(writer, 1, null, null, vectorMaintainer, null));
        }

        @Override
        protected Function<WriterContainer<String>, Boolean> createWriterCleanChecker() {
            return writer -> false;
        }

        @Override
        protected RecordWriter<String> createWriter(
                BinaryRow partition,
                int bucket,
                List<DataFileMeta> restoreFiles,
                long restoredMaxSeqNumber,
                @Nullable CommitIncrement restoreIncrement,
                ExecutorService compactExecutor,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
                boolean ignorePreviousFiles) {
            if (restoredWriter == null) {
                throw new UnsupportedOperationException();
            }
            return restoredWriter;
        }
    }
}
