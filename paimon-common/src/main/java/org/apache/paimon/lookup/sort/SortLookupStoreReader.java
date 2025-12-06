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
import org.apache.paimon.sst.SstFileReader;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

/** A {@link LookupStoreReader} backed by an {@link SstFileReader}. */
public class SortLookupStoreReader implements LookupStoreReader {

    private final FileIO fileIO;
    private final SeekableInputStream input;
    private final SstFileReader sstFileReader;

    public SortLookupStoreReader(
            Comparator<MemorySlice> comparator, File file, CacheManager cacheManager)
            throws IOException {
        final Path filePath = new Path(file.getAbsolutePath());
        this.fileIO = LocalFileIO.create();
        this.input = fileIO.newInputStream(filePath);
        this.sstFileReader =
                new SstFileReader(comparator, file.length(), filePath, input, cacheManager);
    }

    @Nullable
    @Override
    public byte[] lookup(byte[] key) throws IOException {
        return sstFileReader.lookup(key);
    }

    @Override
    public void close() throws IOException {
        // be careful about the close order
        sstFileReader.close();
        input.close();
        fileIO.close();
    }
}
