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

import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

/** Wrap a {@link Iterator} as an {@link RecordReader}. */
public class IteratorRecordReader<T> implements RecordReader<T> {

    private final Iterator<T> iterator;

    private boolean read = false;

    public IteratorRecordReader(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        if (read) {
            return null;
        }

        read = true;
        return new RecordIterator<T>() {
            @Override
            public T next() {
                return iterator.hasNext() ? iterator.next() : null;
            }

            @Override
            public void releaseBatch() {}
        };
    }

    @Override
    public void close() throws IOException {
        if (iterator instanceof AutoCloseable) {
            try {
                ((AutoCloseable) iterator).close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}
