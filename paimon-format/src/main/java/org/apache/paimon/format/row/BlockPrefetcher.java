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

package org.apache.paimon.format.row;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Prefetches and decompresses blocks ahead of consumption.
 *
 * <p>Two IO strategies depending on whether the stream supports positional reads:
 *
 * <ul>
 *   <li>{@link VectoredReadable}: adjacent blocks are coalesced into merged ranges and prefetched
 *       concurrently using thread-safe {@code preadFully}.
 *   <li>Otherwise: blocks are read sequentially in a single background thread (async serial
 *       read-ahead).
 * </ul>
 */
class BlockPrefetcher implements Closeable {

    private final SeekableInputStream inputStream;
    private final ReadStrategy strategy;

    BlockPrefetcher(SeekableInputStream inputStream, RowBlockIndex blockIndex, int[] blocksToRead) {
        this.inputStream = inputStream;
        if (inputStream instanceof VectoredReadable) {
            this.strategy =
                    new VectoredReadStrategy(
                            (VectoredReadable) inputStream, blockIndex, blocksToRead);
        } else {
            this.strategy = new SequentialReadStrategy(inputStream, blockIndex, blocksToRead);
        }
    }

    byte[] nextBlock() throws IOException {
        return strategy.nextBlock();
    }

    int currentBlockIdx() {
        return strategy.currentBlockIdx();
    }

    @Override
    public void close() throws IOException {
        strategy.close();
        inputStream.close();
    }

    static byte[] awaitFuture(CompletableFuture<byte[]> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for prefetch", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Prefetch failed", cause);
        }
    }
}
