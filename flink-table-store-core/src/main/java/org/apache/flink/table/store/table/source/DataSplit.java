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

package org.apache.flink.table.store.table.source;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.DataFileMetaSerializer;
import org.apache.flink.table.store.file.utils.SerializationUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Input splits. Needed by most batch computation engines. */
public class DataSplit implements Split {

    private static final long serialVersionUID = 2L;

    private long snapshotId;
    private BinaryRowData partition;
    private int bucket;
    private List<DataFileMeta> files;
    private boolean isIncremental;

    public DataSplit(
            long snapshotId,
            BinaryRowData partition,
            int bucket,
            List<DataFileMeta> files,
            boolean isIncremental) {
        init(snapshotId, partition, bucket, files, isIncremental);
    }

    private void init(
            long snapshotId,
            BinaryRowData partition,
            int bucket,
            List<DataFileMeta> files,
            boolean isIncremental) {
        this.snapshotId = snapshotId;
        this.partition = partition;
        this.bucket = bucket;
        this.files = files;
        this.isIncremental = isIncremental;
    }

    public long snapshotId() {
        return snapshotId;
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

    public boolean isIncremental() {
        return isIncremental;
    }

    @Override
    public long rowCount() {
        long rowCount = 0;
        for (DataFileMeta file : files) {
            rowCount += file.rowCount();
        }
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
        DataSplit split = (DataSplit) o;
        return bucket == split.bucket
                && Objects.equals(partition, split.partition)
                && Objects.equals(files, split.files)
                && isIncremental == split.isIncremental;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket, files, isIncremental);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        DataSplit split = DataSplit.deserialize(new DataInputViewStreamWrapper(in));
        init(split.snapshotId, split.partition, split.bucket, split.files, split.isIncremental);
    }

    public void serialize(DataOutputView out) throws IOException {
        out.writeLong(snapshotId);
        SerializationUtils.serializeBinaryRow(partition, out);
        out.writeInt(bucket);
        out.writeInt(files.size());
        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();
        for (DataFileMeta file : files) {
            dataFileSer.serialize(file, out);
        }
        out.writeBoolean(isIncremental);
    }

    public static DataSplit deserialize(DataInputView in) throws IOException {
        long snapshotId = in.readLong();
        BinaryRowData partition = SerializationUtils.deserializeBinaryRow(in);
        int bucket = in.readInt();
        int fileNumber = in.readInt();
        List<DataFileMeta> files = new ArrayList<>(fileNumber);
        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();
        for (int i = 0; i < fileNumber; i++) {
            files.add(dataFileSer.deserialize(in));
        }
        return new DataSplit(snapshotId, partition, bucket, files, in.readBoolean());
    }
}
