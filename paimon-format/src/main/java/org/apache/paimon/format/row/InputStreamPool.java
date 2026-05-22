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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/** A lazy pool of {@link SeekableInputStream} instances that opens streams on demand. */
class InputStreamPool implements Closeable {

    private final FileIO fileIO;
    private final Path path;
    private final int maxSize;
    private final AtomicInteger created;
    private final LinkedBlockingQueue<SeekableInputStream> available;

    InputStreamPool(FileIO fileIO, Path path, int maxSize, SeekableInputStream initialStream) {
        this.fileIO = fileIO;
        this.path = path;
        this.maxSize = maxSize;
        this.created = new AtomicInteger(1);
        this.available = new LinkedBlockingQueue<>();
        this.available.add(initialStream);
    }

    SeekableInputStream borrow() throws IOException {
        SeekableInputStream in = available.poll();
        if (in != null) {
            return in;
        }
        if (created.getAndIncrement() < maxSize) {
            return fileIO.newInputStream(path);
        }
        created.decrementAndGet();
        try {
            return available.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for stream", e);
        }
    }

    void returnStream(SeekableInputStream in) {
        available.add(in);
    }

    @Override
    public void close() throws IOException {
        SeekableInputStream in;
        while ((in = available.poll()) != null) {
            in.close();
        }
    }
}
