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

package org.apache.paimon.format;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.util.Map;

/** the context for creating RecordReader {@link RecordReader}. */
public class FormatReaderContext implements FormatReaderFactory.Context {

    private final FileIO fileIO;
    private final Path file;
    private final long fileSize;
    @Nullable private final RoaringBitmap32 selection;
    @Nullable private final ReadBatchSizer readBatchSizer;
    @Nullable private final Map<Path, Object> metadataCache;

    public FormatReaderContext(
            FileIO fileIO,
            Path file,
            long fileSize,
            @Nullable RoaringBitmap32 selection,
            @Nullable ReadBatchSizer readBatchSizer) {
        this(fileIO, file, fileSize, selection, readBatchSizer, null);
    }

    public FormatReaderContext(
            FileIO fileIO,
            Path file,
            long fileSize,
            @Nullable RoaringBitmap32 selection,
            @Nullable ReadBatchSizer readBatchSizer,
            @Nullable Map<Path, Object> metadataCache) {
        this.fileIO = fileIO;
        this.file = file;
        this.fileSize = fileSize;
        this.selection = selection;
        this.readBatchSizer = readBatchSizer;
        this.metadataCache = metadataCache;
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    @Override
    public Path filePath() {
        return file;
    }

    @Override
    public long fileSize() {
        return fileSize;
    }

    @Nullable
    @Override
    public RoaringBitmap32 selection() {
        return selection;
    }

    @Nullable
    @Override
    public ReadBatchSizer readBatchSizer() {
        return readBatchSizer;
    }

    @Nullable
    @Override
    public Map<Path, Object> metadataCache() {
        return metadataCache;
    }
}
