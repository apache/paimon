/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.mapred;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileMetaSerializer;
import org.apache.flink.table.store.file.utils.SerializationUtils;
import org.apache.flink.table.store.table.source.Split;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * {@link FileSplit} for table store. It contains all files to read from a certain partition and
 * bucket.
 */
public class TableStoreInputSplit extends FileSplit {

    private static final String[] ANYWHERE = new String[] {"*"};

    private BinaryRowData partition;
    private int bucket;
    private List<DataFileMeta> files;
    private String bucketPath;

    // public no-argument constructor for deserialization
    public TableStoreInputSplit() {}

    public TableStoreInputSplit(
            BinaryRowData partition, int bucket, List<DataFileMeta> files, String bucketPath) {
        this.partition = partition;
        this.bucket = bucket;
        this.files = files;
        this.bucketPath = bucketPath;
    }

    public static TableStoreInputSplit create(Split split) {
        return new TableStoreInputSplit(
                split.partition(), split.bucket(), split.files(), split.bucketPath().toString());
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

    @Override
    public Path getPath() {
        return new Path(bucketPath);
    }

    @Override
    public long getStart() {
        return 0;
    }

    @Override
    public long getLength() {
        return files.stream().mapToLong(DataFileMeta::fileSize).sum();
    }

    @Override
    public String[] getLocations() {
        return ANYWHERE;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(bucket);

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(bos);
            SerializationUtils.serializeBinaryRow(partition, view);
            DataFileMetaSerializer metaSerializer = new DataFileMetaSerializer();
            metaSerializer.serializeList(files, view);
            byte[] bytes = bos.toByteArray();
            dataOutput.writeInt(bytes.length);
            dataOutput.write(bytes);
        }

        byte[] bucketPathBytes = bucketPath.getBytes();
        dataOutput.writeInt(bucketPathBytes.length);
        dataOutput.write(bucketPathBytes);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        bucket = dataInput.readInt();

        byte[] fieldBytes = new byte[dataInput.readInt()];
        dataInput.readFully(fieldBytes);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(fieldBytes)) {
            DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(bis);
            partition = SerializationUtils.deserializeBinaryRow(view);
            DataFileMetaSerializer metaSerializer = new DataFileMetaSerializer();
            files = metaSerializer.deserializeList(view);
        }

        byte[] bucketPathBytes = new byte[dataInput.readInt()];
        dataInput.readFully(bucketPathBytes);
        bucketPath = new String(bucketPathBytes);
    }

    @Override
    public String toString() {
        return "{"
                + String.join(
                        ", ",
                        Arrays.asList(
                                "partition: " + partition.toString(),
                                "bucket: " + bucket,
                                "files: " + files.toString(),
                                "bucketPath: " + bucketPath))
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TableStoreInputSplit)) {
            return false;
        }
        TableStoreInputSplit that = (TableStoreInputSplit) o;
        return Objects.equals(partition, that.partition)
                && bucket == that.bucket
                && Objects.equals(files, that.files)
                && Objects.equals(bucketPath, that.bucketPath);
    }
}
