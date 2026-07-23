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

import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Function;

/** A {@link RecordReader} whose records expose vector-search scores and row identifiers. */
public interface ScoreRecordReader<T> extends RecordReader<T> {

    @Nullable
    @Override
    ScoreRecordIterator<T> readBatch() throws IOException;

    @Override
    default <R> ScoreRecordReader<R> transform(Function<T, R> function) {
        ScoreRecordReader<T> thisReader = this;
        return new ScoreRecordReader<R>() {
            @Nullable
            @Override
            public ScoreRecordIterator<R> readBatch() throws IOException {
                ScoreRecordIterator<T> iterator = thisReader.readBatch();
                return iterator == null ? null : iterator.transform(function);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }

    @Override
    default ScoreRecordReader<T> filter(Filter<T> filter) {
        ScoreRecordReader<T> thisReader = this;
        return new ScoreRecordReader<T>() {
            @Nullable
            @Override
            public ScoreRecordIterator<T> readBatch() throws IOException {
                ScoreRecordIterator<T> iterator = thisReader.readBatch();
                return iterator == null ? null : iterator.filter(filter);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }
}
