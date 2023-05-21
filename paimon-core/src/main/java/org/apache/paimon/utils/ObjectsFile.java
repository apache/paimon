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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.FileUtils.createFormatReader;

/** A file which contains several {@link T}s, provides read and write. */
public abstract class ObjectsFile<T> {

    protected final FileIO fileIO;
    protected final ObjectSerializer<T> serializer;
    protected final FormatReaderFactory readerFactory;
    protected final PathFactory pathFactory;

    @Nullable private final ObjectsCache<String, T> cache;

    protected ObjectsFile(
            FileIO fileIO,
            ObjectSerializer<T> serializer,
            FormatReaderFactory readerFactory,
            PathFactory pathFactory,
            @Nullable SegmentsCache<String> cache) {
        this.fileIO = fileIO;
        this.serializer = serializer;
        this.readerFactory = readerFactory;
        this.pathFactory = pathFactory;
        this.cache =
                cache == null ? null : new ObjectsCache<>(cache, serializer, this::createIterator);
    }

    public List<T> read(String fileName) {
        return read(fileName, Filter.alwaysTrue(), Filter.alwaysTrue());
    }

    public List<T> read(
            String fileName, Filter<InternalRow> loadFilter, Filter<InternalRow> readFilter) {
        try {
            if (cache != null) {
                return cache.read(fileName, loadFilter, readFilter);
            }

            RecordReader<InternalRow> reader =
                    createFormatReader(fileIO, readerFactory, pathFactory.toPath(fileName));
            if (readFilter != Filter.ALWAYS_TRUE) {
                reader = reader.filter(readFilter);
            }
            List<T> result = new ArrayList<>();
            reader.forEachRemaining(row -> result.add(serializer.fromRow(row)));
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read manifest list " + fileName, e);
        }
    }

    private CloseableIterator<InternalRow> createIterator(String fileName) {
        try {
            return createFormatReader(fileIO, readerFactory, pathFactory.toPath(fileName))
                    .toCloseableIterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(String fileName) {
        fileIO.deleteQuietly(pathFactory.toPath(fileName));
    }
}
