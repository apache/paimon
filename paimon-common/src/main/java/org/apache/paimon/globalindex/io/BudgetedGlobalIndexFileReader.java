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
        SeekableInputStream input = delegate.getInputStream(meta);
        if (queryContext.isUnlimited()) {
            return input;
        }
        return input instanceof VectoredReadable
                ? new BudgetedVectoredInputStream(input, queryContext)
                : new BudgetedSeekableInputStream(input, queryContext);
    }

    private static class BudgetedSeekableInputStream extends SeekableInputStreamWrapper {

        protected final GlobalIndexQueryContext queryContext;

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
    }

    private static final class BudgetedVectoredInputStream extends BudgetedSeekableInputStream
            implements VectoredReadable {

        private final VectoredReadable vectored;

        private BudgetedVectoredInputStream(
                SeekableInputStream input, GlobalIndexQueryContext queryContext) {
            super(input, queryContext);
            this.vectored = (VectoredReadable) input;
        }

        @Override
        public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
            queryContext.reserveReadBytes(length);
            return vectored.pread(position, buffer, offset, length);
        }

        @Override
        public void readVectored(List<? extends FileRange> ranges) throws IOException {
            long totalBytes = 0;
            for (FileRange range : ranges) {
                totalBytes = Math.addExact(totalBytes, range.getLength());
            }
            queryContext.reserveReadBytes(totalBytes);
            vectored.readVectored(ranges);
        }

        @Override
        public int minSeekForVectorReads() {
            return vectored.minSeekForVectorReads();
        }

        @Override
        public int batchSizeForVectorReads() {
            return vectored.batchSizeForVectorReads();
        }

        @Override
        public int parallelismForVectorReads() {
            return vectored.parallelismForVectorReads();
        }
    }
}
