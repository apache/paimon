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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.table.store.table.source.DataSplit;

import java.util.Objects;

/** {@link SourceSplit} of file store. */
public class FileStoreSourceSplit implements SourceSplit {

    /** The unique ID of the split. Unique within the scope of this source. */
    private final String id;

    private final DataSplit split;

    private final long recordsToSkip;

    public FileStoreSourceSplit(String id, DataSplit split) {
        this(id, split, 0);
    }

    public FileStoreSourceSplit(String id, DataSplit split, long recordsToSkip) {
        this.id = id;
        this.split = split;
        this.recordsToSkip = recordsToSkip;
    }

    public DataSplit split() {
        return split;
    }

    public long recordsToSkip() {
        return recordsToSkip;
    }

    @Override
    public String splitId() {
        return id;
    }

    public FileStoreSourceSplit updateWithRecordsToSkip(long recordsToSkip) {
        return new FileStoreSourceSplit(id, split, recordsToSkip);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileStoreSourceSplit other = (FileStoreSourceSplit) o;
        return Objects.equals(id, other.id)
                && Objects.equals(this.split, other.split)
                && recordsToSkip == other.recordsToSkip;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, split, recordsToSkip);
    }
}
