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

import org.apache.paimon.AbstractFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Input splits. Needed by most batch computation engines. */
public class DataSplit implements Split {

    private static final long serialVersionUID = 3L;

    private long snapshotId = 0;
    private boolean isStreaming = false;
    private List<DataFileMeta> beforeFiles = new ArrayList<>();

    private BinaryRow partition;
    private int bucket = -1;
    private List<DataFileMeta> dataFiles;

    public DataSplit() {}

    public long snapshotId() {
        return snapshotId;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public List<DataFileMeta> beforeFiles() {
        return beforeFiles;
    }

    public List<DataFileMeta> dataFiles() {
        return dataFiles;
    }

    public boolean isStreaming() {
        return isStreaming;
    }

    public OptionalLong getLatestFileCreationEpochMillis() {
        return this.dataFiles.stream().mapToLong(DataFileMeta::creationTimeEpochMillis).max();
    }

    @Override
    public long rowCount() {
        long rowCount = 0;
        for (DataFileMeta file : dataFiles) {
            rowCount += file.rowCount();
        }
        return rowCount;
    }

    @Override
    public Optional<List<RawTableFile>> getRawTableFiles(Table table) {
        if (!(table instanceof AbstractFileStoreTable)) {
            return Optional.empty();
        }
        AbstractFileStoreTable fileStoreTable = (AbstractFileStoreTable) table;
        if (!(fileStoreTable.store() instanceof AbstractFileStore)) {
            return Optional.empty();
        }

        FileStorePathFactory pathFactory =
                ((AbstractFileStore<?>) fileStoreTable.store()).pathFactory();
        String bucketPath = pathFactory.bucketPath(partition, bucket).toString();

        // bucket with only one file can be returned
        if (dataFiles.size() == 1) {
            return Optional.of(
                    Collections.singletonList(makeRawTableFile(bucketPath, dataFiles.get(0))));
        }

        // append only files can be returned
        if (fileStoreTable.schema().primaryKeys().isEmpty()) {
            return Optional.of(makeRawTableFiles(bucketPath));
        }

        // bucket containing only one level (except level 0) can be returned
        Set<Integer> levels =
                dataFiles.stream().map(DataFileMeta::level).collect(Collectors.toSet());
        if (levels.size() == 1 && !levels.contains(0)) {
            return Optional.of(makeRawTableFiles(bucketPath));
        }

        return Optional.empty();
    }

    private List<RawTableFile> makeRawTableFiles(String bucketPath) {
        return dataFiles.stream()
                .map(f -> makeRawTableFile(bucketPath, f))
                .collect(Collectors.toList());
    }

    private RawTableFile makeRawTableFile(String bucketPath, DataFileMeta meta) {
        return new RawTableFile(bucketPath + "/" + meta.fileName(), 0, meta.fileSize());
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
                && Objects.equals(beforeFiles, split.beforeFiles)
                && Objects.equals(dataFiles, split.dataFiles)
                && isStreaming == split.isStreaming;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket, beforeFiles, dataFiles, isStreaming);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        assign(deserialize(new DataInputViewStreamWrapper(in)));
    }

    private void assign(DataSplit other) {
        this.snapshotId = other.snapshotId;
        this.partition = other.partition;
        this.bucket = other.bucket;
        this.beforeFiles = other.beforeFiles;
        this.dataFiles = other.dataFiles;
        this.isStreaming = other.isStreaming;
    }

    public void serialize(DataOutputView out) throws IOException {
        out.writeLong(snapshotId);
        SerializationUtils.serializeBinaryRow(partition, out);
        out.writeInt(bucket);

        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();
        out.writeInt(beforeFiles.size());
        for (DataFileMeta file : beforeFiles) {
            dataFileSer.serialize(file, out);
        }

        out.writeInt(dataFiles.size());
        for (DataFileMeta file : dataFiles) {
            dataFileSer.serialize(file, out);
        }

        out.writeBoolean(isStreaming);
    }

    public static DataSplit deserialize(DataInputView in) throws IOException {
        long snapshotId = in.readLong();
        BinaryRow partition = SerializationUtils.deserializeBinaryRow(in);
        int bucket = in.readInt();

        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();
        int beforeNumber = in.readInt();
        List<DataFileMeta> beforeFiles = new ArrayList<>(beforeNumber);
        for (int i = 0; i < beforeNumber; i++) {
            beforeFiles.add(dataFileSer.deserialize(in));
        }

        int fileNumber = in.readInt();
        List<DataFileMeta> dataFiles = new ArrayList<>(fileNumber);
        for (int i = 0; i < fileNumber; i++) {
            dataFiles.add(dataFileSer.deserialize(in));
        }

        boolean isStreaming = in.readBoolean();

        return builder()
                .withSnapshot(snapshotId)
                .withPartition(partition)
                .withBucket(bucket)
                .withBeforeFiles(beforeFiles)
                .withDataFiles(dataFiles)
                .isStreaming(isStreaming)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link DataSplit}. */
    public static class Builder {

        private final DataSplit split = new DataSplit();

        public Builder withSnapshot(long snapshot) {
            this.split.snapshotId = snapshot;
            return this;
        }

        public Builder withPartition(BinaryRow partition) {
            this.split.partition = partition;
            return this;
        }

        public Builder withBucket(int bucket) {
            this.split.bucket = bucket;
            return this;
        }

        public Builder withBeforeFiles(List<DataFileMeta> beforeFiles) {
            this.split.beforeFiles = beforeFiles;
            return this;
        }

        public Builder withDataFiles(List<DataFileMeta> dataFiles) {
            this.split.dataFiles = dataFiles;
            return this;
        }

        public Builder isStreaming(boolean isStreaming) {
            this.split.isStreaming = isStreaming;
            return this;
        }

        public DataSplit build() {
            checkArgument(split.partition != null);
            checkArgument(split.bucket != -1);
            checkArgument(split.dataFiles != null);

            DataSplit split = new DataSplit();
            split.assign(this.split);
            return split;
        }
    }
}
