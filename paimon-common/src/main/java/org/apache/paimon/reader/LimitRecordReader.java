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
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link RecordReader} that stops after reading a given number of records. */
public final class LimitRecordReader<T> implements RecordReader<T> {

    private final RecordReader<T> reader;
    private final long limit;
    private final AtomicLong recordCount = new AtomicLong(0);

    public LimitRecordReader(RecordReader<T> reader, long limit) {
        checkArgument(limit > 0, "Limit must be positive.");
        this.reader = reader;
        this.limit = limit;
    }

    public static <T> RecordReader<T> limit(RecordReader<T> reader, @Nullable Integer limit) {
        if (limit == null || limit <= 0) {
            return reader;
        }
        return new LimitRecordReader<>(reader, limit);
    }

    @Override
    @Nullable
    public RecordIterator<T> readBatch() throws IOException {
        if (recordCount.get() >= limit) {
            return null;
        }
        RecordIterator<T> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }
        return new LimitRecordIterator<>(iterator);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private class LimitRecordIterator<T> implements RecordIterator<T> {

        private final RecordIterator<T> iterator;

        private LimitRecordIterator(RecordIterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        @Nullable
        public T next() throws IOException {
            if (recordCount.get() >= limit) {
                return null;
            }
            T next = iterator.next();
            if (next != null) {
                recordCount.incrementAndGet();
            }
            return next;
        }

        @Override
        public void releaseBatch() {
            iterator.releaseBatch();
        }
    }
}
