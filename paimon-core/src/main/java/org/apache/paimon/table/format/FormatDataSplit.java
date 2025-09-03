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

import java.util.Objects;

/** {@link FormatDataSplit} for format table. */
public class FormatDataSplit implements Split {

    private static final long serialVersionUID = 1L;

    private final Path filePath;
    private final long offset;
    private final long length;
    @Nullable private final BinaryRow partition;

    public FormatDataSplit(Path filePath, long offset, long length, @Nullable BinaryRow partition) {
        this.filePath = filePath;
        this.offset = offset;
        this.length = length;
        this.partition = partition;
    }

    public Path filePath() {
        return this.filePath;
    }

    public Path dataPath() {
        return filePath;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    public BinaryRow partition() {
        return partition;
    }

    @Override
    public long rowCount() {
        return -1;
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
        return offset == that.offset
                && length == that.length
                && Objects.equals(filePath, that.filePath)
                && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, offset, length, partition);
    }
}
