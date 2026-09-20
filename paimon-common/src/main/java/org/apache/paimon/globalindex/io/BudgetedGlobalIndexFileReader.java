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

package org.apache.paimon.globalindex.io;

import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexQueryContext;

import java.io.IOException;
import java.util.List;

/** Adds storage-read accounting to every scalar global index implementation. */
public final class BudgetedGlobalIndexFileReader implements GlobalIndexFileReader {

    private final GlobalIndexFileReader delegate;
    private final GlobalIndexQueryContext queryContext;

    public BudgetedGlobalIndexFileReader(
            GlobalIndexFileReader delegate, GlobalIndexQueryContext queryContext) {
        this.delegate = delegate;
        this.queryContext = queryContext;
    }

    @Override
    public SeekableInputStream getInputStream(GlobalIndexIOMeta meta) throws IOException {
        return new BudgetedSeekableInputStream(delegate.getInputStream(meta), queryContext);
    }

    private static final class BudgetedSeekableInputStream extends SeekableInputStreamWrapper
            implements VectoredReadable {

        private final GlobalIndexQueryContext queryContext;

        private BudgetedSeekableInputStream(
                SeekableInputStream input, GlobalIndexQueryContext queryContext) {
            super(input);
            this.queryContext = queryContext;
        }

        @Override
        public int read() throws IOException {
            queryContext.reserveReadBytes(1);
            return in.read();
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            queryContext.reserveReadBytes(length);
            return in.read(bytes, offset, length);
        }

        @Override
        public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
            queryContext.reserveReadBytes(length);
            if (in instanceof VectoredReadable) {
                return ((VectoredReadable) in).pread(position, buffer, offset, length);
            }

            synchronized (in) {
                long originalPosition = in.getPos();
                try {
                    in.seek(position);
                    return in.read(buffer, offset, length);
                } finally {
                    in.seek(originalPosition);
                }
            }
        }

        @Override
        public void readVectored(List<? extends FileRange> ranges) throws IOException {
            if (!(in instanceof VectoredReadable)) {
                VectoredReadable.super.readVectored(ranges);
                return;
            }

            long totalBytes = 0;
            for (FileRange range : ranges) {
                totalBytes = Math.addExact(totalBytes, range.getLength());
            }
            queryContext.reserveReadBytes(totalBytes);
            ((VectoredReadable) in).readVectored(ranges);
        }

        @Override
        public int minSeekForVectorReads() {
            return in instanceof VectoredReadable
                    ? ((VectoredReadable) in).minSeekForVectorReads()
                    : VectoredReadable.super.minSeekForVectorReads();
        }

        @Override
        public int batchSizeForVectorReads() {
            return in instanceof VectoredReadable
                    ? ((VectoredReadable) in).batchSizeForVectorReads()
                    : VectoredReadable.super.batchSizeForVectorReads();
        }

        @Override
        public int parallelismForVectorReads() {
            return in instanceof VectoredReadable
                    ? ((VectoredReadable) in).parallelismForVectorReads()
                    : VectoredReadable.super.parallelismForVectorReads();
        }
    }
}
