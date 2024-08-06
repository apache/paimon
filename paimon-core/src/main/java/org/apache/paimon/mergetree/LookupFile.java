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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

import static org.apache.paimon.mergetree.LookupUtils.fileKibiBytes;
import static org.apache.paimon.utils.InternalRowPartitionComputer.toSimpleString;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Lookup file for cache remote file to local. */
public class LookupFile implements Closeable {

    private final File localFile;
    private final DataFileMeta remoteFile;
    private final LookupStoreReader reader;
    private final Runnable callback;

    private boolean isClosed = false;

    public LookupFile(
            File localFile, DataFileMeta remoteFile, LookupStoreReader reader, Runnable callback) {
        this.localFile = localFile;
        this.remoteFile = remoteFile;
        this.reader = reader;
        this.callback = callback;
    }

    @Nullable
    public byte[] get(byte[] key) throws IOException {
        checkArgument(!isClosed);
        return reader.lookup(key);
    }

    public DataFileMeta remoteFile() {
        return remoteFile;
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws IOException {
        reader.close();
        isClosed = true;
        callback.run();
        FileIOUtils.deleteFileOrDirectory(localFile);
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
                lookupFile.close();
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
            String partitionString = toSimpleString(partitionType, partition, "-", 20);
            return String.format("%s-%s-%s", partitionString, bucket, remoteFileName);
        }
    }
}
