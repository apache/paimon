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

import java.util.Objects;

/**
 * Represents a split boundary within a file (e.g., a row group in Parquet or a stripe in ORC).
 *
 * <p>This class contains the byte offset, length, and row count for a portion of a file that can be
 * read independently. This enables finer-grained splitting than file-level splitting.
 *
 * @since 0.9.0
 */
public class FileSplitBoundary {

    private final long offset;
    private final long length;
    private final long rowCount;

    public FileSplitBoundary(long offset, long length, long rowCount) {
        this.offset = offset;
        this.length = length;
        this.rowCount = rowCount;
    }

    /** Byte offset where this split starts in the file. */
    public long offset() {
        return offset;
    }

    /** Byte length of this split. */
    public long length() {
        return length;
    }

    /** Number of rows in this split. */
    public long rowCount() {
        return rowCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSplitBoundary that = (FileSplitBoundary) o;
        return offset == that.offset && length == that.length && rowCount == that.rowCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, length, rowCount);
    }

    @Override
    public String toString() {
        return String.format(
                "FileSplitBoundary{offset=%d, length=%d, rowCount=%d}", offset, length, rowCount);
    }
}
