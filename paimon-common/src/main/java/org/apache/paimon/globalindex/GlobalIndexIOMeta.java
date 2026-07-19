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

package org.apache.paimon.globalindex;

import org.apache.paimon.fs.Path;

import java.util.Arrays;
import java.util.Objects;

/** Index meta for global index. */
public class GlobalIndexIOMeta {

    private final Path filePath;
    private final long fileSize;
    private final byte[] metadata;

    /**
     * Total number of logical rows covered by this index file, i.e. the size of the dense local row
     * id universe {@code [0, rowCount)}, or {@code -1} when unknown. For btree indexes the writer
     * assigns exactly one dense row id per logical row (null rows included), so a reader can derive
     * the non-null rows as the complement of the null bitmap within {@code [0, rowCount)} instead
     * of scanning the whole file.
     */
    private final long rowCount;

    public GlobalIndexIOMeta(Path filePath, long fileSize, byte[] metadata) {
        this(filePath, fileSize, metadata, -1);
    }

    public GlobalIndexIOMeta(Path filePath, long fileSize, byte[] metadata, long rowCount) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.metadata = metadata;
        this.rowCount = rowCount;
    }

    public Path filePath() {
        return filePath;
    }

    public long fileSize() {
        return fileSize;
    }

    public byte[] metadata() {
        return metadata;
    }

    /** Row count of this index file, or {@code -1} when unknown. */
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
        GlobalIndexIOMeta that = (GlobalIndexIOMeta) o;
        return Objects.equals(filePath, that.filePath)
                && fileSize == that.fileSize
                && Arrays.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(filePath, fileSize);
        result = 31 * result + Arrays.hashCode(metadata);
        return result;
    }
}
