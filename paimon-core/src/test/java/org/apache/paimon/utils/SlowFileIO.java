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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** A local {@link FileIO} which adds configurable latency to each file operation. */
public class SlowFileIO extends LocalFileIO {

    public static final String SCHEME = "slow-expire";

    private static final AtomicLong DELAY_MILLIS = new AtomicLong();
    private static final AtomicLong DELAYED_OPERATIONS = new AtomicLong();
    private static final AtomicInteger ACTIVE_OPERATIONS = new AtomicInteger();
    private static final AtomicInteger MAX_ACTIVE_OPERATIONS = new AtomicInteger();
    private static final AtomicReference<Set<String>> UNCHECKED_DELETE_FAILURE_NAMES =
            new AtomicReference<>(Collections.emptySet());
    private static final ConcurrentLinkedQueue<ClassLoader> DELETE_CONTEXT_CLASS_LOADERS =
            new ConcurrentLinkedQueue<>();
    private static final AtomicReference<CountDownLatch> NEXT_DELETE_GATE = new AtomicReference<>();
    private static final AtomicInteger ACTIVE_DELETES = new AtomicInteger();
    private static final AtomicInteger MAX_ACTIVE_DELETES = new AtomicInteger();

    public static void reset() {
        DELAY_MILLIS.set(0);
        DELAYED_OPERATIONS.set(0);
        ACTIVE_OPERATIONS.set(0);
        MAX_ACTIVE_OPERATIONS.set(0);
        UNCHECKED_DELETE_FAILURE_NAMES.set(Collections.emptySet());
        DELETE_CONTEXT_CLASS_LOADERS.clear();
        NEXT_DELETE_GATE.set(null);
        ACTIVE_DELETES.set(0);
        MAX_ACTIVE_DELETES.set(0);
    }

    /** Makes the next delete wait until the returned latch is counted down. */
    public static CountDownLatch holdNextDelete() {
        CountDownLatch gate = new CountDownLatch(1);
        NEXT_DELETE_GATE.set(gate);
        return gate;
    }

    public static int activeDeletes() {
        return ACTIVE_DELETES.get();
    }

    public static int maxActiveDeletes() {
        return MAX_ACTIVE_DELETES.get();
    }

    /** Context class loaders seen by delayed deletes since the last {@link #reset()}. */
    public static List<ClassLoader> deleteContextClassLoaders() {
        return new ArrayList<>(DELETE_CONTEXT_CLASS_LOADERS);
    }

    /** Makes deletes of files with the given names throw an unchecked exception. */
    public static void failDeletesUnchecked(String... fileNames) {
        UNCHECKED_DELETE_FAILURE_NAMES.set(new HashSet<>(Arrays.asList(fileNames)));
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
        MAX_ACTIVE_DELETES.accumulateAndGet(ACTIVE_DELETES.incrementAndGet(), Math::max);
        try {
            CountDownLatch gate = NEXT_DELETE_GATE.getAndSet(null);
            if (gate != null) {
                gate.await();
            }
            if (DELAY_MILLIS.get() > 0) {
                DELETE_CONTEXT_CLASS_LOADERS.add(Thread.currentThread().getContextClassLoader());
            }
            delay();
            if (UNCHECKED_DELETE_FAILURE_NAMES.get().contains(path.getName())) {
                throw new IllegalStateException("Injected delete failure for " + path);
            }
            return super.delete(path, recursive);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } finally {
            ACTIVE_DELETES.decrementAndGet();
        }
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
