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
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileMetaSerializer;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * {@link FileSplit} for table store. It contains all files to read from a certain partition and
 * bucket.
 */
public class TableStoreInputSplit extends FileSplit {

    private static final String[] ANYWHERE = new String[] {"*"};

    private RowType partitionType;
    private RowType keyType;
    private RowType valueType;

    private BinaryRowData partition;
    private int bucket;
    private List<DataFileMeta> files;

    private String bucketPath;

    // public no-argument constructor for deserialization
    public TableStoreInputSplit() {}

    public TableStoreInputSplit(
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            BinaryRowData partition,
            int bucket,
            List<DataFileMeta> files,
            String bucketPath) {
        this.partitionType = partitionType;
        this.keyType = keyType;
        this.valueType = valueType;

        this.partition = partition;
        this.bucket = bucket;
        this.files = files;

        this.bucketPath = bucketPath;
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
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(partitionType);
            oos.writeObject(keyType);
            oos.writeObject(valueType);
            byte[] bytes = bos.toByteArray();
            dataOutput.writeInt(bytes.length);
            dataOutput.write(bytes);
        }

        dataOutput.writeInt(bucket);

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(bos);
            RowDataSerializer partitionSerializer = new RowDataSerializer(partitionType);
            partitionSerializer.serialize(partition, view);
            DataFileMetaSerializer metaSerializer = new DataFileMetaSerializer(keyType, valueType);
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
        byte[] typeBytes = new byte[dataInput.readInt()];
        dataInput.readFully(typeBytes);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(typeBytes)) {
            ObjectInputStream ois = new ObjectInputStream(bis);
            partitionType = (RowType) ois.readObject();
            keyType = (RowType) ois.readObject();
            valueType = (RowType) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }

        bucket = dataInput.readInt();

        byte[] fieldBytes = new byte[dataInput.readInt()];
        dataInput.readFully(fieldBytes);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(fieldBytes)) {
            DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(bis);
            RowDataSerializer partitionSerializer = new RowDataSerializer(partitionType);
            partition = partitionSerializer.toBinaryRow(partitionSerializer.deserialize(view));
            DataFileMetaSerializer metaSerializer = new DataFileMetaSerializer(keyType, valueType);
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
                                "partitionType: " + partitionType.asSummaryString(),
                                "keyType: " + keyType.asSummaryString(),
                                "valueType: " + valueType.asSummaryString(),
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
        return Objects.equals(partitionType, that.partitionType)
                && Objects.equals(keyType, that.keyType)
                && Objects.equals(valueType, that.valueType)
                && Objects.equals(partition, that.partition)
                && bucket == that.bucket
                && Objects.equals(files, that.files)
                && Objects.equals(bucketPath, that.bucketPath);
    }
}
