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

/** A {@link RecordReader} to support returning {@link FileRecordIterator}. */
public interface FileRecordReader<T> extends RecordReader<T> {

    @Override
    @Nullable
    FileRecordIterator<T> readBatch() throws IOException;

    @Override
    default FileRecordReader<T> filter(Filter<T> filter) {
        FileRecordReader<T> thisReader = this;
        return new FileRecordReader<T>() {
            @Nullable
            @Override
            public FileRecordIterator<T> readBatch() throws IOException {
                FileRecordIterator<T> iterator = thisReader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return iterator.filter(filter);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }
}
