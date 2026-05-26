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

import java.io.Serializable;
import java.util.Objects;

/** Byte range and row count of a sub-file split (e.g. Parquet row group, ORC stripe). */
public final class FileSplitBoundary implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long offset;
    private final long length;
    private final long rowCount;

    public FileSplitBoundary(long offset, long length, long rowCount) {
        this.offset = offset;
        this.length = length;
        this.rowCount = rowCount;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    public long rowCount() {
        return rowCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileSplitBoundary)) return false;
        FileSplitBoundary that = (FileSplitBoundary) o;
        return offset == that.offset && length == that.length && rowCount == that.rowCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, length, rowCount);
    }

    @Override
    public String toString() {
        return "FileSplitBoundary{offset=" + offset + ", length=" + length + ", rowCount=" + rowCount + '}';
    }
}
