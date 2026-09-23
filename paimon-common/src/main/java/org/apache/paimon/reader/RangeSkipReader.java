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

package org.apache.paimon.reader;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link RecordReader} wrapper that implements a naive range read: skip the first {@code skip}
 * rows of the delegate's output stream, then return at most {@code limit} rows, and stop early once
 * the limit is reached.
 *
 * <p>This is the fallback implementation for {@code withRowRange} when the underlying format does
 * not support row-range skipping (e.g. avro), for primary-key tables (merge-tree output stream),
 * and for the precise effective-row correction of append tables that carry deletion vectors or
 * filters. It guarantees range-read correctness over the <b>effective output stream</b> (rows
 * already filtered by deletion vectors / predicates), at the cost of decoding skipped rows.
 *
 * <p>Both {@code skip} and {@code limit} are expressed in the effective-row space of the delegate's
 * output. {@code limit} may be {@link Long#MAX_VALUE} to mean "read to the end after skipping".
 */
public class RangeSkipReader<T> implements RecordReader<T> {

    private final RecordReader<T> delegate;

    private final long skip;

    private final long limit;

    private long skipped;

    private long returned;

    private RecordReader.RecordIterator<T> currentBatch;

    public RangeSkipReader(RecordReader<T> delegate, long skip, long limit) {
        this.delegate = delegate;
        this.skip = skip;
        this.limit = limit;
    }

    @Nullable
    @Override
    public RecordReader.RecordIterator<T> readBatch() throws IOException {
        // Already reached the limit: stop early without reading further from the delegate.
        if (returned >= limit) {
            return null;
        }

        // Phase 1: skip the first `skip` effective rows.
        while (skipped < skip) {
            RecordReader.RecordIterator<T> batch =
                    currentBatch == null ? delegate.readBatch() : currentBatch;
            if (batch == null) {
                // delegate exhausted before reaching `skip` rows: nothing to return.
                currentBatch = null;
                return null;
            }
            currentBatch = batch;
            long canSkip = Math.min(skip - skipped, Long.MAX_VALUE);
            long skippedNow = 0;
            T record;
            // Consume records from the batch until we have skipped enough or the batch is drained.
            while (skippedNow < canSkip) {
                record = currentBatch.next();
                if (record == null) {
                    break;
                }
                skippedNow++;
            }
            skipped += skippedNow;
            if (skippedNow == 0) {
                // batch drained, move to next batch
                currentBatch.releaseBatch();
                currentBatch = null;
            }
        }

        // Phase 2: return a limited view of the remaining stream.
        RecordReader.RecordIterator<T> batch =
                currentBatch == null ? delegate.readBatch() : currentBatch;
        if (batch == null) {
            currentBatch = null;
            return null;
        }
        currentBatch = null;
        long remaining = limit - returned;
        return new LimitedRecordIterator<>(batch, remaining, this);
    }

    @Override
    public void close() throws IOException {
        try {
            if (currentBatch != null) {
                currentBatch.releaseBatch();
            }
        } finally {
            delegate.close();
        }
    }

    /** A {@link RecordReader.RecordIterator} that yields at most {@code remaining} records. */
    private static final class LimitedRecordIterator<T> implements RecordReader.RecordIterator<T> {

        private final RecordReader.RecordIterator<T> delegate;

        private long remaining;

        private final RangeSkipReader<T> owner;

        private LimitedRecordIterator(
                RecordReader.RecordIterator<T> delegate, long remaining, RangeSkipReader<T> owner) {
            this.delegate = delegate;
            this.remaining = remaining;
            this.owner = owner;
        }

        @Nullable
        @Override
        public T next() throws IOException {
            if (remaining <= 0) {
                return null;
            }
            T record = delegate.next();
            if (record == null) {
                return null;
            }
            remaining--;
            owner.returned++;
            return record;
        }

        @Override
        public void releaseBatch() {
            delegate.releaseBatch();
        }
    }
}
