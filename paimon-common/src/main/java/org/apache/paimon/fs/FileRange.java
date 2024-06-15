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

package org.apache.paimon.fs;

import java.util.concurrent.CompletableFuture;

/* This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A byte range of a file. */
public interface FileRange {

    /** Get the starting offset of the range. */
    long getOffset();

    /** Get the length of the range. */
    int getLength();

    /** Get the future data for this range. */
    CompletableFuture<byte[]> getData();

    /**
     * Factory method to create a FileRange object.
     *
     * @param offset starting offset of the range.
     * @param length length of the range.
     * @return a new instance of FileRangeImpl.
     */
    static FileRange createFileRange(long offset, int length) {
        return new FileRangeImpl(offset, length);
    }

    /** An implementation for {@link FileRange}. */
    class FileRangeImpl implements FileRange {

        private final long offset;
        private final int length;
        private final CompletableFuture<byte[]> reader;

        public FileRangeImpl(long offset, int length) {
            this.offset = offset;
            this.length = length;
            this.reader = new CompletableFuture<>();
        }

        @Override
        public String toString() {
            return "range[" + offset + "," + (offset + length) + ")";
        }

        @Override
        public long getOffset() {
            return offset;
        }

        @Override
        public int getLength() {
            return length;
        }

        @Override
        public CompletableFuture<byte[]> getData() {
            return reader;
        }
    }
}
