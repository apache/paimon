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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileMetaSerializer;
import org.apache.flink.table.store.file.utils.SerializationUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.store.file.utils.SerializationUtils.deserializedBytes;
import static org.apache.flink.table.store.file.utils.SerializationUtils.serializeBytes;
import static org.apache.flink.util.InstantiationUtil.deserializeObject;
import static org.apache.flink.util.InstantiationUtil.serializeObject;

/** Input splits. Needed by most batch computation engines. */
public class Split {

    private final BinaryRowData partition;
    private final int bucket;
    private final List<DataFileMeta> files;

    private final Path bucketPath;

    public Split(BinaryRowData partition, int bucket, List<DataFileMeta> files, Path bucketPath) {
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

    public Path bucketPath() {
        return bucketPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Split split = (Split) o;
        return bucket == split.bucket
                && Objects.equals(partition, split.partition)
                && Objects.equals(files, split.files)
                && Objects.equals(bucketPath, split.bucketPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket, files, bucketPath);
    }

    public void serialize(DataOutputView out) throws IOException {
        SerializationUtils.serializeBinaryRow(partition, out);
        out.writeInt(bucket);
        out.writeInt(files.size());
        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();
        for (DataFileMeta file : files) {
            dataFileSer.serialize(file, out);
        }
        serializeBytes(out, serializeObject(bucketPath));
    }

    public static Split deserialize(DataInputView in) throws IOException {
        BinaryRowData partition = SerializationUtils.deserializeBinaryRow(in);
        int bucket = in.readInt();
        int fileNumber = in.readInt();
        List<DataFileMeta> files = new ArrayList<>(fileNumber);
        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();
        for (int i = 0; i < fileNumber; i++) {
            files.add(dataFileSer.deserialize(in));
        }
        Path bucketPath;
        try {
            bucketPath =
                    deserializeObject(
                            deserializedBytes(in), Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        return new Split(partition, bucket, files, bucketPath);
    }
}
