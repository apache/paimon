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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.globalindex.wrap.FileIndexReaderWrapper;
import org.apache.paimon.globalindex.wrap.FileIndexWriterWrapper;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Bitmap global index. */
public class BitmapGlobalIndex implements GlobalIndexer {

    private final BitmapFileIndex index;

    public BitmapGlobalIndex(BitmapFileIndex index) {
        this.index = index;
    }

    @Override
    public GlobalIndexSingletonWriter createWriter(GlobalIndexFileWriter fileWriter)
            throws IOException {
        FileIndexWriter writer = index.createWriter();
        return new FileIndexWriterWrapper(
                fileWriter, writer, BitmapGlobalIndexerFactory.IDENTIFIER);
    }

    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files) throws IOException {
        checkArgument(files.size() == 1);
        GlobalIndexIOMeta indexMeta = files.get(0);
        SeekableInputStream input = fileReader.getInputStream(indexMeta.fileName());
        FileIndexReader reader = index.createReader(input, 0, (int) indexMeta.fileSize());
        return new FileIndexReaderWrapper(reader, this::toGlobalResult, input);
    }

    private Optional<GlobalIndexResult> toGlobalResult(FileIndexResult result) {
        if (FileIndexResult.REMAIN == result) {
            return Optional.empty();
        } else if (FileIndexResult.SKIP == result) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        BitmapIndexResult bitmapResult = (BitmapIndexResult) result;
        return Optional.of(GlobalIndexResult.create(() -> bitmapResult.get().toNavigable64()));
    }
}
