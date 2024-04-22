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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Input splits. Needed by most batch computation engines. */
public class DataSplit implements Split {

    private static final long serialVersionUID = 6L;

    private long snapshotId = 0;
    private boolean isStreaming = false;
    private List<DataFileMeta> beforeFiles = new ArrayList<>();
    @Nullable private List<DeletionFile> beforeDeletionFiles;

    private BinaryRow partition;
    private int bucket = -1;
    private List<DataFileMeta> dataFiles;
    @Nullable private List<DeletionFile> dataDeletionFiles;

    private List<RawFile> rawFiles = Collections.emptyList();
    private List<IndexFile> indexFiles = Collections.emptyList();

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

    public Optional<List<DeletionFile>> beforeDeletionFiles() {
        return Optional.ofNullable(beforeDeletionFiles);
    }

    public List<DataFileMeta> dataFiles() {
        return dataFiles;
    }

    @Override
    public Optional<List<DeletionFile>> deletionFiles() {
        return Optional.ofNullable(dataDeletionFiles);
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
    public Optional<List<RawFile>> convertToRawFiles() {
        if (rawFiles.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(rawFiles);
        }
    }

    @Override
    public Optional<List<IndexFile>> indexFiles() {
        if (indexFiles.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(indexFiles);
        }
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
                && Objects.equals(beforeDeletionFiles, split.beforeDeletionFiles)
                && Objects.equals(dataFiles, split.dataFiles)
                && Objects.equals(dataDeletionFiles, split.dataDeletionFiles)
                && isStreaming == split.isStreaming
                && Objects.equals(rawFiles, split.rawFiles)
                && Objects.equals(indexFiles, split.indexFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                partition,
                bucket,
                beforeFiles,
                beforeDeletionFiles,
                dataFiles,
                dataDeletionFiles,
                isStreaming,
                rawFiles,
                indexFiles);
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
        this.beforeDeletionFiles = other.beforeDeletionFiles;
        this.dataFiles = other.dataFiles;
        this.dataDeletionFiles = other.dataDeletionFiles;
        this.isStreaming = other.isStreaming;
        this.rawFiles = other.rawFiles;
        this.indexFiles = other.indexFiles;
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

        DeletionFile.serializeList(out, beforeDeletionFiles);

        out.writeInt(dataFiles.size());
        for (DataFileMeta file : dataFiles) {
            dataFileSer.serialize(file, out);
        }

        DeletionFile.serializeList(out, dataDeletionFiles);

        out.writeBoolean(isStreaming);

        out.writeInt(rawFiles.size());
        for (RawFile rawFile : rawFiles) {
            rawFile.serialize(out);
        }

        IndexFile.serializeList(out, indexFiles);
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

        List<DeletionFile> beforeDeletionFiles = DeletionFile.deserializeList(in);

        int fileNumber = in.readInt();
        List<DataFileMeta> dataFiles = new ArrayList<>(fileNumber);
        for (int i = 0; i < fileNumber; i++) {
            dataFiles.add(dataFileSer.deserialize(in));
        }

        List<DeletionFile> dataDeletionFiles = DeletionFile.deserializeList(in);

        boolean isStreaming = in.readBoolean();

        int rawFileNum = in.readInt();
        List<RawFile> rawFiles = new ArrayList<>();
        for (int i = 0; i < rawFileNum; i++) {
            rawFiles.add(RawFile.deserialize(in));
        }

        List<IndexFile> indexFiles = IndexFile.deserializeList(in);

        DataSplit.Builder builder =
                builder()
                        .withSnapshot(snapshotId)
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withBeforeFiles(beforeFiles)
                        .withDataFiles(dataFiles)
                        .isStreaming(isStreaming)
                        .rawFiles(rawFiles)
                        .indexFiles(indexFiles);
        if (beforeDeletionFiles != null) {
            builder.withBeforeDeletionFiles(beforeDeletionFiles);
        }
        if (dataDeletionFiles != null) {
            builder.withDataDeletionFiles(dataDeletionFiles);
        }
        return builder.build();
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
            this.split.beforeFiles = new ArrayList<>(beforeFiles);
            return this;
        }

        public Builder withBeforeDeletionFiles(List<DeletionFile> beforeDeletionFiles) {
            this.split.beforeDeletionFiles = new ArrayList<>(beforeDeletionFiles);
            return this;
        }

        public Builder withDataFiles(List<DataFileMeta> dataFiles) {
            this.split.dataFiles = new ArrayList<>(dataFiles);
            return this;
        }

        public Builder withDataDeletionFiles(List<DeletionFile> dataDeletionFiles) {
            this.split.dataDeletionFiles = new ArrayList<>(dataDeletionFiles);
            return this;
        }

        public Builder isStreaming(boolean isStreaming) {
            this.split.isStreaming = isStreaming;
            return this;
        }

        public Builder rawFiles(List<RawFile> rawFiles) {
            this.split.rawFiles = rawFiles;
            return this;
        }

        public Builder indexFiles(List<IndexFile> indexFiles) {
            this.split.indexFiles = indexFiles;
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
