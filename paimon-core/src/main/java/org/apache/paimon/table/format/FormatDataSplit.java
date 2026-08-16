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

package org.apache.paimon.table.format;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.source.Split;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * {@link Split} for format table. A split may contain multiple files packed by {@code
 * source.split.target-size}, so a single reader task can read several files sequentially.
 */
public class FormatDataSplit implements Split {

    private static final long serialVersionUID = 3L;

    private final List<FileMeta> files;
    @Nullable private final BinaryRow partition;

    public FormatDataSplit(List<FileMeta> files, @Nullable BinaryRow partition) {
        this.files = files;
        this.partition = partition;
    }

    public List<FileMeta> files() {
        return files;
    }

    @Nullable
    public BinaryRow partition() {
        return partition;
    }

    /** Total bytes to read for this split, i.e. the sum of {@link FileMeta#readSize()}. */
    public long totalSize() {
        return files.stream().mapToLong(FileMeta::readSize).sum();
    }

    /** Number of files (or file ranges) in this split. */
    public int fileCount() {
        return files.size();
    }

    @Override
    public long rowCount() {
        return -1;
    }

    @Override
    public OptionalLong mergedRowCount() {
        return OptionalLong.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FormatDataSplit that = (FormatDataSplit) o;
        return Objects.equals(files, that.files) && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(files, partition);
    }

    /**
     * A single file (or one offset range of a splittable file) inside a {@link FormatDataSplit}.
     */
    public static class FileMeta implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Path filePath;
        private final long fileSize;
        private final long offset;
        // If null, means reading the whole file.
        @Nullable private final Long length;

        public FileMeta(Path filePath, long fileSize, long offset, @Nullable Long length) {
            this.filePath = filePath;
            this.fileSize = fileSize;
            this.offset = offset;
            this.length = length;
        }

        public FileMeta(Path filePath, long fileSize) {
            this(filePath, fileSize, 0L, null);
        }

        public Path filePath() {
            return filePath;
        }

        public long fileSize() {
            return fileSize;
        }

        public long offset() {
            return offset;
        }

        @Nullable
        public Long length() {
            return length;
        }

        /** Bytes this segment actually reads: range length when sliced, otherwise whole file. */
        public long readSize() {
            return length != null ? length : fileSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileMeta that = (FileMeta) o;
            return fileSize == that.fileSize
                    && offset == that.offset
                    && Objects.equals(length, that.length)
                    && Objects.equals(filePath, that.filePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filePath, fileSize, offset, length);
        }
    }
}
