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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A split describes chain table read scope. It follows DataSplit's custom serialization pattern and
 * extends it with branch + partition per data file.
 */
public class ChainSplit implements Split {

    private static final long serialVersionUID = 1L;

    private static final int VERSION = 1;

    private BinaryRow logicalPartition;
    private List<DataFileMeta> dataFiles;
    private Map<String, String> fileBranchMapping;
    private Map<String, String> fileBucketPathMapping;

    public ChainSplit(
            BinaryRow logicalPartition,
            List<DataFileMeta> dataFiles,
            Map<String, String> fileBranchMapping,
            Map<String, String> fileBucketPathMapping) {
        this.logicalPartition = logicalPartition;
        this.dataFiles = dataFiles;
        this.fileBranchMapping = fileBranchMapping;
        this.fileBucketPathMapping = fileBucketPathMapping;
    }

    public BinaryRow logicalPartition() {
        return logicalPartition;
    }

    public List<DataFileMeta> dataFiles() {
        return dataFiles;
    }

    public Map<String, String> fileBranchMapping() {
        return fileBranchMapping;
    }

    public Map<String, String> fileBucketPathMapping() {
        return fileBucketPathMapping;
    }

    @Override
    public long rowCount() {
        long sum = 0;
        for (DataFileMeta file : dataFiles) {
            sum += file.rowCount();
        }
        return sum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChainSplit that = (ChainSplit) o;
        return Objects.equals(logicalPartition, that.logicalPartition)
                && Objects.equals(dataFiles, that.dataFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalPartition, dataFiles);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        assign(deserialize(new DataInputViewStreamWrapper(in)));
    }

    protected void assign(ChainSplit other) {
        this.logicalPartition = other.logicalPartition;
        this.dataFiles = other.dataFiles;
        this.fileBranchMapping = other.fileBranchMapping;
        this.fileBucketPathMapping = other.fileBucketPathMapping;
    }

    public void serialize(DataOutputView out) throws IOException {
        out.writeInt(VERSION);

        SerializationUtils.serializeBinaryRow(logicalPartition, out);

        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();
        int size = dataFiles == null ? 0 : dataFiles.size();
        out.writeInt(size);
        if (size > 0) {
            for (DataFileMeta file : dataFiles) {
                dataFileSer.serialize(file, out);
            }
        }

        out.writeInt(fileBucketPathMapping.size());
        for (Map.Entry<String, String> entry : fileBucketPathMapping.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
        out.writeInt(fileBranchMapping.size());
        for (Map.Entry<String, String> entry : fileBranchMapping.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    public static ChainSplit deserialize(DataInputView in) throws IOException {
        int version = in.readInt();
        if (version != VERSION) {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }

        BinaryRow logicalPartition = SerializationUtils.deserializeBinaryRow(in);

        int n = in.readInt();
        List<DataFileMeta> dataFiles = new ArrayList<>(n);
        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();
        for (int i = 0; i < n; i++) {
            dataFiles.add(dataFileSer.deserialize(in));
        }

        int size = in.readInt();
        HashMap<String, String> fileBucketPathMapping = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            fileBucketPathMapping.put(key, value);
        }
        size = in.readInt();
        HashMap<String, String> fileBranchMapping = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            fileBranchMapping.put(key, value);
        }

        return new ChainSplit(
                logicalPartition, dataFiles, fileBucketPathMapping, fileBranchMapping);
    }
}
