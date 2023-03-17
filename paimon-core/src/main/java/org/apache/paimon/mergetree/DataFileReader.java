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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.FileIOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/** Reader for a {@link DataFileMeta}. */
public class DataFileReader implements Closeable {

    private final File localFile;
    private final DataFileMeta remoteFile;
    private final LookupStoreReader reader;

    public DataFileReader(File localFile, DataFileMeta remoteFile, LookupStoreReader reader) {
        this.localFile = localFile;
        this.remoteFile = remoteFile;
        this.reader = reader;
    }

    @Nullable
    public byte[] get(byte[] key) throws IOException {
        return reader.lookup(key);
    }

    public int fileKibiBytes() {
        long kibiBytes = localFile.length() >> 10;
        if (kibiBytes > Integer.MAX_VALUE) {
            throw new RuntimeException(
                    "Lookup file is too big: " + MemorySize.ofKibiBytes(kibiBytes));
        }
        return (int) kibiBytes;
    }

    public DataFileMeta remoteFile() {
        return remoteFile;
    }

    @Override
    public void close() throws IOException {
        reader.close();
        FileIOUtils.deleteFileOrDirectory(localFile);
    }
}
