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

package org.apache.paimon.utils;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** A local {@link FileIO} which adds configurable latency to each file operation. */
public class SlowFileIO extends LocalFileIO {

    public static final String SCHEME = "slow-expire";

    private static final AtomicLong DELAY_MILLIS = new AtomicLong();
    private static final AtomicLong DELAYED_OPERATIONS = new AtomicLong();
    private static final AtomicInteger ACTIVE_OPERATIONS = new AtomicInteger();
    private static final AtomicInteger MAX_ACTIVE_OPERATIONS = new AtomicInteger();

    public static void reset() {
        DELAY_MILLIS.set(0);
        DELAYED_OPERATIONS.set(0);
        ACTIVE_OPERATIONS.set(0);
        MAX_ACTIVE_OPERATIONS.set(0);
    }

    public static void setDelayMillis(long delayMillis) {
        DELAY_MILLIS.set(delayMillis);
    }

    public static long delayedOperations() {
        return DELAYED_OPERATIONS.get();
    }

    public static int maxActiveOperations() {
        return MAX_ACTIVE_OPERATIONS.get();
    }

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        delay();
        return super.newInputStream(path);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        delay();
        return super.newOutputStream(path, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        delay();
        return super.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        delay();
        return super.listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        delay();
        return super.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        delay();
        return super.delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        delay();
        return super.mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        delay();
        return super.rename(src, dst);
    }

    private void delay() throws IOException {
        long delayMillis = DELAY_MILLIS.get();
        if (delayMillis <= 0) {
            return;
        }

        DELAYED_OPERATIONS.incrementAndGet();
        int active = ACTIVE_OPERATIONS.incrementAndGet();
        updateMaxActiveOperations(active);
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } finally {
            ACTIVE_OPERATIONS.decrementAndGet();
        }
    }

    private static void updateMaxActiveOperations(int active) {
        int current;
        do {
            current = MAX_ACTIVE_OPERATIONS.get();
            if (active <= current) {
                return;
            }
        } while (!MAX_ACTIVE_OPERATIONS.compareAndSet(current, active));
    }

    /** Loader for {@link SlowFileIO}. */
    public static class Loader implements FileIOLoader {

        private static final long serialVersionUID = 1L;

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileIO load(Path path) {
            return new SlowFileIO();
        }
    }
}
