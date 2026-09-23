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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.sst.BlockCache;
import org.apache.paimon.sst.SstFileReader;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FileBasedBloomFilter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

/** A {@link LookupStoreReader} backed by an {@link SstFileReader}. */
public class SortLookupStoreReader implements LookupStoreReader {

    private final SeekableInputStream input;
    private final SstFileReader reader;

    public SortLookupStoreReader(
            Comparator<MemorySlice> comparator,
            Path filePath,
            long fileLen,
            SeekableInputStream input,
            CacheManager cacheManager) {
        this.input = input;
        BlockCache blockCache = new BlockCache(filePath, input, cacheManager);
        int footerLen = SortLookupStoreFooter.ENCODED_LENGTH;
        MemorySegment footerData =
                blockCache.getBlock(fileLen - footerLen, footerLen, b -> b, true);
        SortLookupStoreFooter footer =
                SortLookupStoreFooter.readFooter(MemorySlice.wrap(footerData).toInput());
        FileBasedBloomFilter bloomFilter =
                FileBasedBloomFilter.create(
                        input, filePath, cacheManager, footer.getBloomFilterHandle());
        this.reader =
                new SstFileReader(
                        comparator, blockCache, footer.getIndexBlockHandle(), bloomFilter);
    }

    @Nullable
    @Override
    public byte[] lookup(byte[] key) throws IOException {
        return reader.lookup(key);
    }

    public SstFileReader.SstFileIterator createIterator() {
        return reader.createIterator();
    }

    /**
     * Close the underlying input stream without invalidating blocks in the shared cache.
     *
     * <p>The reader must not be used after this method returns.
     */
    public void closeInput() throws IOException {
        input.close();
    }

    @Override
    public void close() throws IOException {
        // input is the file handle. Both callers of this method -- LocalKvDb#closeAndDeleteSstFile
        // and the shutdown loop in LocalKvDb#close -- deliberately catch and log so that one bad
        // reader cannot stall shutdown, which is exactly why a descriptor abandoned here would
        // never be reclaimed and would show up only as a warning in the log.
        Throwable collected = null;
        try {
            reader.close();
        } catch (Throwable t) {
            collected = ExceptionUtils.firstOrSuppressed(t, collected);
        }
        try {
            input.close();
        } catch (Throwable t) {
            collected = ExceptionUtils.firstOrSuppressed(t, collected);
        }
        if (collected != null) {
            rethrowAsIOException(collected);
        }
    }

    private static void rethrowAsIOException(Throwable failure) throws IOException {
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        throw new IOException(failure);
    }
}
