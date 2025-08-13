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
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SerializationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/** A specific implementation about {@link DataSplit} for chain table. */
public class ChainDataSplit extends DataSplit {

    public static final Logger LOG = LoggerFactory.getLogger(ChainDataSplit.class);
    public static final int VIRTUAL_SNAPSHOT = -1;
    public static final String VIRTUAL_BUCKET_PATH = "placeholder::virtual-bucket-path";

    private boolean allSnapshotSplit;

    private BinaryRow readPartition;

    private HashMap<String, String> fileBucketPathMapping;

    private HashMap<String, String> fileBranchMapping;

    public ChainDataSplit(
            BinaryRow readPartition,
            int bucket,
            List<DataSplit> splits,
            HashMap<String, String> fileBucketPathMapping,
            HashMap<String, String> fileBranchMapping,
            boolean allSnapshotSplit) {
        this.allSnapshotSplit = allSnapshotSplit;
        this.readPartition = readPartition;
        this.fileBucketPathMapping = fileBucketPathMapping;
        this.fileBranchMapping = fileBranchMapping;
        DataSplit split =
                splits.size() > 1 ? mergeSplits(readPartition, bucket, splits) : splits.get(0);
        assign(split);
    }

    public ChainDataSplit(
            DataSplit split,
            BinaryRow readPartition,
            HashMap<String, String> fileBucketPathMapping,
            HashMap<String, String> fileBranchMapping,
            boolean allSnapshotSplit) {
        this.allSnapshotSplit = allSnapshotSplit;
        this.readPartition = readPartition;
        this.fileBucketPathMapping = fileBucketPathMapping;
        this.fileBranchMapping = fileBranchMapping;
        assign(split);
    }

    public DataSplit mergeSplits(BinaryRow readPartition, int bucket, List<DataSplit> splits) {
        List<Integer> totalBucketSet =
                splits.stream()
                        .map(split -> split.totalBuckets())
                        .distinct()
                        .collect(Collectors.toList());
        if (totalBucketSet.size() != 1) {
            throw new IllegalStateException(
                    String.format(
                            "totalBuckets must be same, " + "but got %s, bucket %s",
                            totalBucketSet, bucket));
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
            List<DeletionFile> currentDeleteFiles = split.dataDeletionFiles();
            if (currentDeleteFiles != null) {
                dataDeletionFiles.addAll(currentDeleteFiles);
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
        boolean isStreaming = splits.stream().allMatch(split -> (split.isStreaming()));
        boolean rawConvertible =
                allSnapshotSplit && splits.stream().allMatch(split -> split.rawConvertible());
        DataSplit.Builder dataSplitBuilder =
                DataSplit.builder()
                        .withSnapshot(VIRTUAL_SNAPSHOT)
                        .withPartition(readPartition)
                        .withBucket(bucket)
                        .withBucketPath(VIRTUAL_BUCKET_PATH)
                        .withTotalBuckets(totalBucketSet.get(0))
                        .withBeforeFiles(beforeFiles)
                        .withDataFiles(dataFiles)
                        .isStreaming(isStreaming)
                        .rawConvertible(rawConvertible);
        if (beforeDeletionFiles != null) {
            dataSplitBuilder.withBeforeDeletionFiles(beforeDeletionFiles);
        }
        if (dataDeletionFiles != null) {
            dataSplitBuilder.withDataDeletionFiles(dataDeletionFiles);
        }
        return dataSplitBuilder.build();
    }

    @Override
    public BinaryRow readPartition() {
        return readPartition;
    }

    @Override
    public HashMap<String, String> fileBucketPathMapping() {
        return fileBucketPathMapping;
    }

    @Override
    public HashMap<String, String> fileBranchMapping() {
        return fileBranchMapping;
    }

    public boolean isAllSnapshotSplit() {
        return allSnapshotSplit;
    }

    @Override
    public boolean equals(Object o) {
        boolean isSame = super.equals(o);
        if (isSame) {
            ChainDataSplit chainDataSplit = (ChainDataSplit) o;
            return Objects.equals(readPartition, chainDataSplit.readPartition())
                    && allSnapshotSplit == chainDataSplit.isAllSnapshotSplit();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), readPartition, allSnapshotSplit);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        ChainDataSplit split = deserialize(new DataInputViewStreamWrapper(in));
        this.allSnapshotSplit = split.isAllSnapshotSplit();
        this.readPartition = split.readPartition();
        this.fileBucketPathMapping = split.fileBucketPathMapping();
        this.fileBranchMapping = split.fileBranchMapping();
        assign(split);
    }

    @Override
    public void serialize(DataOutputView out) throws IOException {
        super.serialize(out);
        out.writeBoolean(allSnapshotSplit);
        SerializationUtils.serializeBinaryRow(readPartition, out);
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

    public static ChainDataSplit deserialize(DataInputView in) throws IOException {
        DataSplit dataSplit = DataSplit.deserialize(in);
        boolean allSnapshotSplit = in.readBoolean();
        BinaryRow readPartition = SerializationUtils.deserializeBinaryRow(in);
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
        return new ChainDataSplit(
                dataSplit,
                readPartition,
                fileBucketPathMapping,
                fileBranchMapping,
                allSnapshotSplit);
    }

    @Override
    public String dataSplitType() {
        return DataSplitType.CHAIN_DATA_SPLIT.name();
    }
}
