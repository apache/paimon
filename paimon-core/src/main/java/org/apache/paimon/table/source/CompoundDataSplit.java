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
import org.apache.paimon.utils.FunctionWithIOException;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** A split for contains multiple data split cross branch and partition. */
public class CompoundDataSplit implements Split {

    private static final long serialVersionUID = 1L;
    private static final long MAGIC = SplitContext.DATA_SPLIT_MAGIC;
    private static final int VERSION = SplitContext.DATA_SPLIT_VERSION;

    private BinaryRow readPartition;
    private HashMap<String, String> fileBucketPathMapping;
    private HashMap<String, String> fileBranchMapping;
    private List<DataFileMeta> dataFiles;

    private boolean rawConvertible;

    public CompoundDataSplit(
            BinaryRow readPartition,
            List<DataFileMeta> dataFiles,
            HashMap<String, String> fileBucketPathMapping,
            HashMap<String, String> fileBranchMapping,
            boolean rawConvertible) {
        this.readPartition = readPartition;
        this.dataFiles = dataFiles;
        this.fileBucketPathMapping = fileBucketPathMapping;
        this.fileBranchMapping = fileBranchMapping;
        this.rawConvertible = rawConvertible;
    }

    public static CompoundDataSplit createCompoundSplit(
            BinaryRow readPartition,
            List<DataSplit> splits,
            HashMap<String, String> fileBucketPathMapping,
            HashMap<String, String> fileBranchMapping,
            boolean isCompound) {
        List<Integer> totalBucketSet =
                splits.stream()
                        .map(split -> split.totalBuckets())
                        .distinct()
                        .collect(Collectors.toList());
        if (totalBucketSet.size() != 1) {
            throw new IllegalStateException(
                    String.format("totalBuckets must be same, but got %s", totalBucketSet));
        }

        boolean allDeleteFilesNull =
                splits.stream().map(DataSplit::deletionFiles).allMatch(Objects::isNull);
        List<DataFileMeta> dataFiles = new ArrayList<>();
        List<DeletionFile> dataDeletionFiles = new ArrayList<>();
        boolean allBeforeDeleteFilesNull =
                splits.stream()
                        .map(DataSplit::beforeDeletionFiles)
                        .allMatch(beforeDeletionFilesOpt -> !beforeDeletionFilesOpt.isPresent());
        List<DataFileMeta> beforeFiles = new ArrayList<>();
        List<DeletionFile> beforeDeletionFiles = new ArrayList<>();
        for (DataSplit split : splits) {
            List<DataFileMeta> currentFiles = split.dataFiles();
            dataFiles.addAll(currentFiles);
            Optional<List<DeletionFile>> currentDeleteFiles = split.deletionFiles();
            if (currentDeleteFiles.isPresent()) {
                dataDeletionFiles.addAll(currentDeleteFiles.get());
            } else if (!allDeleteFilesNull) {
                for (int i = 0; i < currentFiles.size(); i++) {
                    dataDeletionFiles.add(null);
                }
            }
            List<DataFileMeta> currentBeforeFiles = split.beforeFiles();
            beforeFiles.addAll(currentBeforeFiles);
            Optional<List<DeletionFile>> currentBeforeDeletionFiles = split.beforeDeletionFiles();
            if (currentBeforeDeletionFiles.isPresent()) {
                beforeDeletionFiles.addAll(currentBeforeDeletionFiles.get());
            } else if (!allBeforeDeleteFilesNull) {
                for (int i = 0; i < currentBeforeFiles.size(); i++) {
                    beforeDeletionFiles.add(null);
                }
            }
        }
        dataDeletionFiles = allDeleteFilesNull ? null : dataDeletionFiles;
        beforeDeletionFiles = allBeforeDeleteFilesNull ? null : beforeDeletionFiles;
        Preconditions.checkArgument(
                dataDeletionFiles == null || dataDeletionFiles.size() == dataFiles.size(),
                "dataDeletionFiles size must be null or equal to dataFiles size");
        Preconditions.checkArgument(
                (beforeFiles == null || beforeFiles.isEmpty())
                        && (beforeDeletionFiles == null || beforeDeletionFiles.isEmpty()),
                "beforeFiles and beforeDeletionFiles must be null at the same time");
        boolean allBatch = splits.stream().allMatch(split -> (!split.isStreaming()));
        boolean allRawConvertible = splits.stream().allMatch(split -> split.rawConvertible());
        boolean rawConvertible = !isCompound && allBatch && allRawConvertible;
        return new CompoundDataSplit(
                readPartition, dataFiles, fileBucketPathMapping, fileBranchMapping, rawConvertible);
    }

    public BinaryRow readPartition() {
        return readPartition;
    }

    public List<DataFileMeta> dataFiles() {
        return dataFiles;
    }

    public HashMap<String, String> fileBucketPathMapping() {
        return fileBucketPathMapping;
    }

    public HashMap<String, String> fileBranchMapping() {
        return fileBranchMapping;
    }

    public boolean rawConvertible() {
        return rawConvertible;
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
        return Optional.empty();
    }

    @Override
    public Optional<List<DeletionFile>> deletionFiles() {
        return Optional.empty();
    }

    @Override
    public Optional<List<IndexFile>> indexFiles() {
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompoundDataSplit split = (CompoundDataSplit) o;
        return Objects.equals(readPartition, split.readPartition)
                && Objects.equals(dataFiles, split.dataFiles)
                && rawConvertible == split.rawConvertible;
    }

    @Override
    public int hashCode() {
        return Objects.hash(readPartition, dataFiles, rawConvertible);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        CompoundDataSplit split = deserialize(new DataInputViewStreamWrapper(in));
        this.readPartition = split.readPartition();
        this.dataFiles = split.dataFiles();
        this.fileBucketPathMapping = split.fileBucketPathMapping();
        this.fileBranchMapping = split.fileBranchMapping();
        this.rawConvertible = split.rawConvertible();
    }

    public void serialize(DataOutputView out) throws IOException {
        out.writeLong(MAGIC);
        out.writeInt(VERSION);
        SerializationUtils.serializeBinaryRow(readPartition, out);
        out.writeInt(dataFiles.size());
        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();
        for (DataFileMeta file : dataFiles) {
            dataFileSer.serialize(file, out);
        }
        out.writeBoolean(rawConvertible);
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

    public static CompoundDataSplit deserialize(DataInputView in) throws IOException {
        long magic = in.readLong();
        int version = magic == MAGIC ? in.readInt() : 1;
        BinaryRow readPartition = SerializationUtils.deserializeBinaryRow(in);
        int fileNumber = in.readInt();
        List<DataFileMeta> dataFiles = new ArrayList<>(fileNumber);
        FunctionWithIOException<DataInputView, DataFileMeta> dataFileSer =
                DataSplit.getFileMetaSerde(version);
        for (int i = 0; i < fileNumber; i++) {
            dataFiles.add(dataFileSer.apply(in));
        }
        boolean rawConvertible = in.readBoolean();
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
        return new CompoundDataSplit(
                readPartition, dataFiles, fileBucketPathMapping, fileBranchMapping, rawConvertible);
    }
}
