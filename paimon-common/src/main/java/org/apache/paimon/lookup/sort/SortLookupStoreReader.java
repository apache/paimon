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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.sst.BlockCache;
import org.apache.paimon.sst.SstFileLookupReader;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

/** A {@link LookupStoreReader} for sort store. */
public class SortLookupStoreReader implements LookupStoreReader {

    private final SstFileLookupReader sstFileLookupReader;
    private final FileIO fileIO;
    private final SeekableInputStream inputStream;

    public SortLookupStoreReader(
            Comparator<MemorySlice> comparator, File file, int blockSize, CacheManager cacheManager)
            throws IOException {
        final Path filePath = new Path(file.getAbsolutePath());

        this.fileIO = LocalFileIO.create();
        this.inputStream = fileIO.newInputStream(filePath);
        this.sstFileLookupReader =
                new SstFileLookupReader(
                        inputStream,
                        comparator,
                        file.length(),
                        filePath,
                        new BlockCache(filePath, inputStream, cacheManager));
    }

    @Nullable
    @Override
    public byte[] lookup(byte[] key) throws IOException {
        return sstFileLookupReader.lookup(key);
    }

    @Override
    public void close() throws IOException {
        sstFileLookupReader.close();
        inputStream.close();
        fileIO.close();
    }
}
