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
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;

import java.util.List;
import java.util.Objects;

/** {@link SourceSplit} of file store. */
public class FileStoreSourceSplit implements SourceSplit {

    /** The unique ID of the split. Unique within the scope of this source. */
    private final String id;

    private final BinaryRowData partition;

    private final int bucket;

    private final List<DataFileMeta> files;

    private final long recordsToSkip;

    public FileStoreSourceSplit(
            String id, BinaryRowData partition, int bucket, List<DataFileMeta> files) {
        this(id, partition, bucket, files, 0);
    }

    public FileStoreSourceSplit(
            String id,
            BinaryRowData partition,
            int bucket,
            List<DataFileMeta> files,
            long recordsToSkip) {
        this.id = id;
        this.partition = partition;
        this.bucket = bucket;
        this.files = files;
        this.recordsToSkip = recordsToSkip;
    }

    public BinaryRowData partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public List<DataFileMeta> files() {
        return files;
    }

    public long recordsToSkip() {
        return recordsToSkip;
    }

    @Override
    public String splitId() {
        return id;
    }

    public FileStoreSourceSplit updateWithRecordsToSkip(long recordsToSkip) {
        return new FileStoreSourceSplit(id, partition, bucket, files, recordsToSkip);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileStoreSourceSplit split = (FileStoreSourceSplit) o;
        return bucket == split.bucket
                && recordsToSkip == split.recordsToSkip
                && Objects.equals(id, split.id)
                && Objects.equals(partition, split.partition)
                && Objects.equals(files, split.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, partition, bucket, files, recordsToSkip);
    }
}
