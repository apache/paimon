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

package org.apache.paimon.format.parquet;

import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.ParquetFileRange;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A {@link SeekableInputStream} for paimon. */
public class ParquetInputStream extends DelegatingSeekableInputStream {

    private final SeekableInputStream in;

    public ParquetInputStream(SeekableInputStream in) {
        super(in);
        this.in = in;
    }

    public SeekableInputStream in() {
        return in;
    }

    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
        in.seek(newPos);
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator)
            throws IOException {
        if (!(in instanceof VectoredReadable)) {
            throw new UnsupportedOperationException("Vectored IO is not supported for " + this);
        }
        List<FileRange> adaptedRanges = new ArrayList<>(ranges.size());
        for (ParquetFileRange parquetRange : ranges) {
            adaptedRanges.add(
                    FileRange.createFileRange(parquetRange.getOffset(), parquetRange.getLength()));
        }
        ((VectoredReadable) in).readVectored(adaptedRanges);
        for (int i = 0; i < adaptedRanges.size(); i++) {
            FileRange fileRange = adaptedRanges.get(i);
            ParquetFileRange parquetRange = ranges.get(i);
            CompletableFuture<ByteBuffer> byteBufferFuture =
                    fileRange.getData().thenApply(bytes -> ByteBuffer.wrap(bytes));
            parquetRange.setDataReadFuture(byteBufferFuture);
        }
    }
}
