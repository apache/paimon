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

package org.apache.paimon.reader;

import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Function;

/**
 * A {@link RecordReader.RecordIterator} to support returning the record's row position and file
 * Path.
 */
public interface FileRecordIterator<T> extends RecordReader.RecordIterator<T> {

    /**
     * Get the row position of the row returned by {@link RecordReader.RecordIterator#next}.
     *
     * @return the row position from 0 to the number of rows in the file
     */
    long returnedPosition();

    /** @return the file path */
    Path filePath();

    @Override
    default <R> FileRecordIterator<R> transform(Function<T, R> function) {
        FileRecordIterator<T> thisIterator = this;
        return new FileRecordIterator<R>() {
            @Override
            public long returnedPosition() {
                return thisIterator.returnedPosition();
            }

            @Override
            public Path filePath() {
                return thisIterator.filePath();
            }

            @Nullable
            @Override
            public R next() throws IOException {
                T next = thisIterator.next();
                if (next == null) {
                    return null;
                }
                return function.apply(next);
            }

            @Override
            public void releaseBatch() {
                thisIterator.releaseBatch();
            }
        };
    }

    @Override
    default FileRecordIterator<T> filter(Filter<T> filter) {
        FileRecordIterator<T> thisIterator = this;
        return new FileRecordIterator<T>() {
            @Override
            public long returnedPosition() {
                return thisIterator.returnedPosition();
            }

            @Override
            public Path filePath() {
                return thisIterator.filePath();
            }

            @Nullable
            @Override
            public T next() throws IOException {
                while (true) {
                    T next = thisIterator.next();
                    if (next == null) {
                        return null;
                    }
                    if (filter.test(next)) {
                        return next;
                    }
                }
            }

            @Override
            public void releaseBatch() {
                thisIterator.releaseBatch();
            }
        };
    }
}
