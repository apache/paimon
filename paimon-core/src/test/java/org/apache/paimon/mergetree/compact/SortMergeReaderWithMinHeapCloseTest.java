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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * {@link SortMergeReaderWithMinHeap#close()} releases one reader per sorted run being merged. It
 * used to do so as bare {@code close()} calls across three loops, so the first one to throw
 * abandoned every reader behind it — on the compaction path, where each reader holds an open data
 * file.
 */
class SortMergeReaderWithMinHeapCloseTest {

    @Test
    void closeReleasesEveryReaderWhenAnEarlierOneFails() throws Exception {
        // Readers that never produce a batch stay in nextBatchReaders until close(), which is the
        // first of the three loops.
        RecordingReader first = new RecordingReader(new IOException("first reader"));
        RecordingReader second = new RecordingReader(null);
        RecordingReader third = new RecordingReader(new IOException("third reader"));

        SortMergeReaderWithMinHeap<KeyValue> reader = newReader(first, second, third);

        Throwable thrown = catchThrowable(reader::close);
        assertThat(thrown).isInstanceOf(IOException.class).hasMessage("first reader");

        // Every reader was still asked to close...
        assertThat(first.closed).isTrue();
        assertThat(second.closed).isTrue();
        assertThat(third.closed).isTrue();
        // ...and the later failure rode along instead of replacing the first one.
        assertThat(thrown.getSuppressed()).hasSize(1);
        assertThat(thrown.getSuppressed()[0]).hasMessage("third reader");
    }

    @Test
    void closeIsSilentWhenEveryReaderCloses() throws Exception {
        RecordingReader first = new RecordingReader(null);
        RecordingReader second = new RecordingReader(null);

        newReader(first, second).close();

        assertThat(first.closed).isTrue();
        assertThat(second.closed).isTrue();
    }

    @Test
    void closeStillReleasesReadersWhenAnIteratorFailsToRelease() throws Exception {
        // A reader that yields one record ends up as an Element in minHeap, so close() goes
        // through releaseBatch() before closing it — the second of the three loops.
        RecordingReader heaped = new RecordingReader(null, new IllegalStateException("release"));
        RecordingReader plain = new RecordingReader(null);

        SortMergeReaderWithMinHeap<KeyValue> reader = newReader(heaped, plain);
        reader.readBatch();

        assertThatThrownBy(reader::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("release");

        assertThat(heaped.closed).isTrue();
        assertThat(plain.closed).isTrue();
    }

    private static SortMergeReaderWithMinHeap<KeyValue> newReader(RecordingReader... readers) {
        return new SortMergeReaderWithMinHeap<>(
                new ArrayList<>(Arrays.asList(readers)),
                Comparator.comparingInt(row -> row.getInt(0)),
                null,
                new MergeFunctionWrapper<KeyValue>() {
                    @Override
                    public void reset() {}

                    @Override
                    public void add(KeyValue kv) {}

                    @Override
                    public KeyValue getResult() {
                        return null;
                    }
                });
    }

    /**
     * A reader that optionally yields a single record, then fails on {@code close()} and/or on
     * {@code releaseBatch()} as configured.
     */
    private static class RecordingReader implements RecordReader<KeyValue> {

        private final IOException closeFailure;
        private final RuntimeException releaseFailure;
        private boolean batchServed;
        private boolean closed;

        RecordingReader(IOException closeFailure) {
            this(closeFailure, null);
        }

        RecordingReader(IOException closeFailure, RuntimeException releaseFailure) {
            this.closeFailure = closeFailure;
            this.releaseFailure = releaseFailure;
            // Only readers configured to fail on release need to reach the heap; the rest stay in
            // nextBatchReaders so the first close() loop is the one under test.
            this.batchServed = releaseFailure == null;
        }

        @Override
        public RecordIterator<KeyValue> readBatch() {
            if (batchServed) {
                return null;
            }
            batchServed = true;
            return new RecordIterator<KeyValue>() {
                private boolean served;

                @Override
                public KeyValue next() {
                    if (served) {
                        return null;
                    }
                    served = true;
                    return new KeyValue()
                            .replace(GenericRow.of(1), 1L, RowKind.INSERT, GenericRow.of(1));
                }

                @Override
                public void releaseBatch() {
                    if (releaseFailure != null) {
                        throw releaseFailure;
                    }
                }
            };
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
