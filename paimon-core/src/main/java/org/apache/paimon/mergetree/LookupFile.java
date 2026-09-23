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

package org.apache.paimon.mergetree;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.mergetree.LookupUtils.fileKibiBytes;
import static org.apache.paimon.utils.InternalRowPartitionComputer.partToSimpleString;

/** Lookup file for cache remote file to local. */
public class LookupFile {

    private static final Logger LOG = LoggerFactory.getLogger(LookupFile.class);

    private final File localFile;
    private final int level;
    private final long schemaId;
    private final String serVersion;
    private final LookupStoreReader reader;
    private final Runnable callback;

    private final AtomicLong requestCount = new AtomicLong();
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public LookupFile(
            File localFile,
            int level,
            long schemaId,
            String serVersion,
            LookupStoreReader reader,
            Runnable callback) {
        this.localFile = localFile;
        this.level = level;
        this.schemaId = schemaId;
        this.serVersion = serVersion;
        this.reader = reader;
        this.callback = callback;
    }

    public File localFile() {
        return localFile;
    }

    public long schemaId() {
        return schemaId;
    }

    public String serVersion() {
        return serVersion;
    }

    @Nullable
    public synchronized byte[] get(byte[] key) throws IOException {
        if (isClosed.get()) {
            return null;
        }
        requestCount.incrementAndGet();
        byte[] res = reader.lookup(key);
        if (res != null) {
            hitCount.incrementAndGet();
        }
        return res;
    }

    public int level() {
        return level;
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    public synchronized void close(RemovalCause cause) throws IOException {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }

        Throwable throwable = null;
        try {
            reader.close();
        } catch (Throwable t) {
            throwable = t;
        }

        try {
            callback.run();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        LOG.info(
                "Delete Lookup file {} due to {}. Access stats: requestCount={}, hitCount={}, size={}KB",
                localFile.getName(),
                cause,
                requestCount.get(),
                hitCount.get(),
                localFile.length() >> 10);

        try {
            FileIOUtils.deleteFileOrDirectory(localFile);
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        throwIfNotNull(throwable);
    }

    private static Throwable addSuppressed(@Nullable Throwable throwable, Throwable suppressed) {
        if (throwable == null) {
            return suppressed;
        }
        throwable.addSuppressed(suppressed);
        return throwable;
    }

    private static void throwIfNotNull(@Nullable Throwable throwable) throws IOException {
        if (throwable == null) {
            return;
        }
        if (throwable instanceof IOException) {
            throw (IOException) throwable;
        }
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        throw new IOException(throwable);
    }

    // ==================== Cache for Local File ======================

    public static Cache<String, LookupFile> createCache(
            Duration fileRetention, MemorySize maxDiskSize) {
        return Caffeine.newBuilder()
                .expireAfterAccess(fileRetention)
                .maximumWeight(maxDiskSize.getKibiBytes())
                .weigher(LookupFile::fileWeigh)
                .removalListener(LookupFile::removalCallback)
                .executor(Runnable::run)
                .build();
    }

    private static int fileWeigh(String file, LookupFile lookupFile) {
        return fileKibiBytes(lookupFile.localFile);
    }

    private static void removalCallback(String file, LookupFile lookupFile, RemovalCause cause) {
        if (lookupFile != null) {
            try {
                lookupFile.close(cause);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static String localFilePrefix(
            RowType partitionType, BinaryRow partition, int bucket, String remoteFileName) {
        if (partition.getFieldCount() == 0) {
            return String.format("%s-%s", bucket, remoteFileName);
        } else {
            String partitionString = partToSimpleString(partitionType, partition, "-", 20);
            return String.format("%s-%s-%s", partitionString, bucket, remoteFileName);
        }
    }
}
