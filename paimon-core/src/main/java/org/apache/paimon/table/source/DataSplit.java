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
import org.apache.paimon.io.DataFileMeta08Serializer;
import org.apache.paimon.io.DataFileMeta09Serializer;
import org.apache.paimon.io.DataFileMeta10LegacySerializer;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.FunctionWithIOException;
import org.apache.paimon.utils.SerializationUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFilePathFactory.INDEX_PATH_SUFFIX;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Input splits. Needed by most batch computation engines. */
public class DataSplit implements Split {

    private static final long serialVersionUID = 7L;
    private static final long MAGIC = -2394839472490812314L;
    private static final int VERSION = 5;

    private long snapshotId = 0;
    private BinaryRow partition;
    private int bucket = -1;
    private String bucketPath;

    private List<DataFileMeta> beforeFiles = new ArrayList<>();
    @Nullable private List<DeletionFile> beforeDeletionFiles;

    private List<DataFileMeta> dataFiles;
    @Nullable private List<DeletionFile> dataDeletionFiles;

    private boolean isStreaming = false;
    private boolean rawConvertible;

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

    public String bucketPath() {
        return bucketPath;
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

    public boolean rawConvertible() {
        return rawConvertible;
    }

    public OptionalLong latestFileCreationEpochMillis() {
        return this.dataFiles.stream().mapToLong(DataFileMeta::creationTimeEpochMillis).max();
    }

    public OptionalLong earliestFileCreationEpochMillis() {
        return this.dataFiles.stream().mapToLong(DataFileMeta::creationTimeEpochMillis).min();
    }

    @Override
    public long rowCount() {
        long rowCount = 0;
        for (DataFileMeta file : dataFiles) {
            rowCount += file.rowCount();
        }
        return rowCount;
    }

    /** Whether it is possible to calculate the merged row count. */
    public boolean mergedRowCountAvailable() {
        return rawConvertible
                && (dataDeletionFiles == null
                        || dataDeletionFiles.stream()
                                .allMatch(f -> f == null || f.cardinality() != null));
    }

    public long mergedRowCount() {
        checkState(mergedRowCountAvailable());
        return partialMergedRowCount();
    }

    /**
     * Obtain merged row count as much as possible. There are two scenarios where accurate row count
     * can be calculated:
     *
     * <p>1. raw file and no deletion file.
     *
     * <p>2. raw file + deletion file with cardinality.
     */
    public long partialMergedRowCount() {
        long sum = 0L;
        if (rawConvertible) {
            List<RawFile> rawFiles = convertToRawFiles().orElse(null);
            if (rawFiles != null) {
                for (int i = 0; i < rawFiles.size(); i++) {
                    RawFile rawFile = rawFiles.get(i);
                    if (dataDeletionFiles == null || dataDeletionFiles.get(i) == null) {
                        sum += rawFile.rowCount();
                    } else if (dataDeletionFiles.get(i).cardinality() != null) {
                        sum += rawFile.rowCount() - dataDeletionFiles.get(i).cardinality();
                    }
                }
            }
        }
        return sum;
    }

    @Override
    public Optional<List<RawFile>> convertToRawFiles() {
        if (rawConvertible) {
            return Optional.of(
                    dataFiles.stream()
                            .map(f -> makeRawTableFile(bucketPath, f))
                            .collect(Collectors.toList()));
        } else {
            return Optional.empty();
        }
    }

    private RawFile makeRawTableFile(String bucketPath, DataFileMeta file) {
        String path = file.externalPath();
        if (path == null) {
            path = bucketPath + "/" + file.fileName();
        } else {
            path = path + "/" + file.fileName();
        }
        return new RawFile(
                path,
                file.fileSize(),
                0,
                file.fileSize(),
                file.fileFormat(),
                file.schemaId(),
                file.rowCount());
    }

    @Override
    @Nullable
    public Optional<List<IndexFile>> indexFiles() {
        List<IndexFile> indexFiles = new ArrayList<>();
        boolean hasIndexFile = false;
        for (DataFileMeta file : dataFiles) {
            List<String> exFiles =
                    file.extraFiles().stream()
                            .filter(s -> s.endsWith(INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());
            if (exFiles.isEmpty()) {
                indexFiles.add(null);
            } else if (exFiles.size() == 1) {
                hasIndexFile = true;
                indexFiles.add(new IndexFile(bucketPath + "/" + exFiles.get(0)));
            } else {
                throw new RuntimeException(
                        "Wrong number of file index for file "
                                + file.fileName()
                                + " index files: "
                                + String.join(",", exFiles));
            }
        }

        return hasIndexFile ? Optional.of(indexFiles) : Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataSplit dataSplit = (DataSplit) o;
        return snapshotId == dataSplit.snapshotId
                && bucket == dataSplit.bucket
                && isStreaming == dataSplit.isStreaming
                && rawConvertible == dataSplit.rawConvertible
                && Objects.equals(partition, dataSplit.partition)
                && Objects.equals(bucketPath, dataSplit.bucketPath)
                && Objects.equals(beforeFiles, dataSplit.beforeFiles)
                && Objects.equals(beforeDeletionFiles, dataSplit.beforeDeletionFiles)
                && Objects.equals(dataFiles, dataSplit.dataFiles)
                && Objects.equals(dataDeletionFiles, dataSplit.dataDeletionFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                snapshotId,
                partition,
                bucket,
                bucketPath,
                beforeFiles,
                beforeDeletionFiles,
                dataFiles,
                dataDeletionFiles,
                isStreaming,
                rawConvertible);
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
        this.bucketPath = other.bucketPath;
        this.beforeFiles = other.beforeFiles;
        this.beforeDeletionFiles = other.beforeDeletionFiles;
        this.dataFiles = other.dataFiles;
        this.dataDeletionFiles = other.dataDeletionFiles;
        this.isStreaming = other.isStreaming;
        this.rawConvertible = other.rawConvertible;
    }

    public void serialize(DataOutputView out) throws IOException {
        out.writeLong(MAGIC);
        out.writeInt(VERSION);
        out.writeLong(snapshotId);
        SerializationUtils.serializeBinaryRow(partition, out);
        out.writeInt(bucket);
        out.writeUTF(bucketPath);

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

        out.writeBoolean(rawConvertible);
    }

    public static DataSplit deserialize(DataInputView in) throws IOException {
        long magic = in.readLong();
        int version = magic == MAGIC ? in.readInt() : 1;
        // version 1 does not write magic number in, so the first long is snapshot id.
        long snapshotId = version == 1 ? magic : in.readLong();
        BinaryRow partition = SerializationUtils.deserializeBinaryRow(in);
        int bucket = in.readInt();
        String bucketPath = in.readUTF();

        FunctionWithIOException<DataInputView, DataFileMeta> dataFileSer =
                getFileMetaSerde(version);
        FunctionWithIOException<DataInputView, DeletionFile> deletionFileSerde =
                getDeletionFileSerde(version);
        int beforeNumber = in.readInt();
        List<DataFileMeta> beforeFiles = new ArrayList<>(beforeNumber);
        for (int i = 0; i < beforeNumber; i++) {
            beforeFiles.add(dataFileSer.apply(in));
        }

        List<DeletionFile> beforeDeletionFiles =
                DeletionFile.deserializeList(in, deletionFileSerde);

        int fileNumber = in.readInt();
        List<DataFileMeta> dataFiles = new ArrayList<>(fileNumber);
        for (int i = 0; i < fileNumber; i++) {
            dataFiles.add(dataFileSer.apply(in));
        }

        List<DeletionFile> dataDeletionFiles = DeletionFile.deserializeList(in, deletionFileSerde);

        boolean isStreaming = in.readBoolean();
        boolean rawConvertible = in.readBoolean();

        DataSplit.Builder builder =
                builder()
                        .withSnapshot(snapshotId)
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withBucketPath(bucketPath)
                        .withBeforeFiles(beforeFiles)
                        .withDataFiles(dataFiles)
                        .isStreaming(isStreaming)
                        .rawConvertible(rawConvertible);

        if (beforeDeletionFiles != null) {
            builder.withBeforeDeletionFiles(beforeDeletionFiles);
        }
        if (dataDeletionFiles != null) {
            builder.withDataDeletionFiles(dataDeletionFiles);
        }
        return builder.build();
    }

    private static FunctionWithIOException<DataInputView, DataFileMeta> getFileMetaSerde(
            int version) {
        if (version == 1) {
            DataFileMeta08Serializer serializer = new DataFileMeta08Serializer();
            return serializer::deserialize;
        } else if (version == 2) {
            DataFileMeta09Serializer serializer = new DataFileMeta09Serializer();
            return serializer::deserialize;
        } else if (version == 3 || version == 4) {
            DataFileMeta10LegacySerializer serializer = new DataFileMeta10LegacySerializer();
            return serializer::deserialize;
        } else if (version >= 5) {
            DataFileMetaSerializer serializer = new DataFileMetaSerializer();
            return serializer::deserialize;
        } else {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }
    }

    private static FunctionWithIOException<DataInputView, DeletionFile> getDeletionFileSerde(
            int version) {
        if (version >= 1 && version <= 3) {
            return DeletionFile::deserializeV3;
        } else if (version >= 4) {
            return DeletionFile::deserialize;
        } else {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }
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

        public Builder withBucketPath(String bucketPath) {
            this.split.bucketPath = bucketPath;
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

        public Builder rawConvertible(boolean rawConvertible) {
            this.split.rawConvertible = rawConvertible;
            return this;
        }

        public DataSplit build() {
            checkArgument(split.partition != null);
            checkArgument(split.bucket != -1);
            checkArgument(split.bucketPath != null);
            checkArgument(split.dataFiles != null);

            DataSplit split = new DataSplit();
            split.assign(this.split);
            return split;
        }
    }
}
