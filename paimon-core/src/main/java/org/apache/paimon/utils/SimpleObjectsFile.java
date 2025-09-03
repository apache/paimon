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
import org.apache.paimon.data.Segments;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

/** A file which contains several {@link T}s, provides read and write. */
public abstract class SimpleObjectsFile<T>
        extends ObjectsFile<T, ObjectsCache.Filters<T>, Segments> {

    public SimpleObjectsFile(
            FileIO fileIO,
            ObjectSerializer<T> serializer,
            RowType formatType,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory,
            @Nullable SegmentsCache<Path> cache) {
        super(
                fileIO,
                serializer,
                formatType,
                readerFactory,
                writerFactory,
                compression,
                pathFactory,
                cache);
    }

    @Override
    protected SimpleObjectsCache<Path, T> createCache(
            SegmentsCache<Path> cache, RowType formatType) {
        return new SimpleObjectsCache<>(
                cache, serializer, formatType, super::fileSize, super::createIterator);
    }

    @Override
    protected ObjectsCache.Filters<T> createFilters(
            Filter<InternalRow> readFilter, Filter<T> readTFilter) {
        return new ObjectsCache.Filters<>(readFilter, readTFilter);
    }
}
