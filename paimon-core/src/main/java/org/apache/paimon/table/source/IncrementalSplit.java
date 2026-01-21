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
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.FunctionWithIOException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Incremental split for batch and streaming. */
public class IncrementalSplit implements Split {

    private static final long serialVersionUID = 1L;

    private static final int VERSION = 1;

    private long snapshotId;
    private BinaryRow partition;
    private int bucket;
    private int totalBuckets;

    private List<DataFileMeta> beforeFiles;
    private @Nullable List<DeletionFile> beforeDeletionFiles;

    private List<DataFileMeta> afterFiles;
    private @Nullable List<DeletionFile> afterDeletionFiles;

    private boolean isStreaming;

    public IncrementalSplit(
            long snapshotId,
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            List<DataFileMeta> beforeFiles,
            @Nullable List<DeletionFile> beforeDeletionFiles,
            List<DataFileMeta> afterFiles,
            @Nullable List<DeletionFile> afterDeletionFiles,
            boolean isStreaming) {
        this.snapshotId = snapshotId;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.beforeFiles = beforeFiles;
        this.beforeDeletionFiles = beforeDeletionFiles;
        this.afterFiles = afterFiles;
        this.afterDeletionFiles = afterDeletionFiles;
        this.isStreaming = isStreaming;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public List<DataFileMeta> beforeFiles() {
        return beforeFiles;
    }

    @Nullable
    public List<DeletionFile> beforeDeletionFiles() {
        return beforeDeletionFiles;
    }

    public List<DataFileMeta> afterFiles() {
        return afterFiles;
    }

    @Nullable
    public List<DeletionFile> afterDeletionFiles() {
        return afterDeletionFiles;
    }

    public boolean isStreaming() {
        return isStreaming;
    }

    @Override
    public long rowCount() {
        return 0;
    }

    @Override
    public OptionalLong mergedRowCount() {
        return OptionalLong.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IncrementalSplit that = (IncrementalSplit) o;
        return snapshotId == that.snapshotId
                && bucket == that.bucket
                && totalBuckets == that.totalBuckets
                && isStreaming == that.isStreaming
                && Objects.equals(partition, that.partition)
                && Objects.equals(beforeFiles, that.beforeFiles)
                && Objects.equals(beforeDeletionFiles, that.beforeDeletionFiles)
                && Objects.equals(afterFiles, that.afterFiles)
                && Objects.equals(afterDeletionFiles, that.afterDeletionFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                snapshotId,
                partition,
                bucket,
                totalBuckets,
                beforeFiles,
                beforeDeletionFiles,
                afterFiles,
                afterDeletionFiles,
                isStreaming);
    }

    @Override
    public String toString() {
        return "IncrementalSplit{"
                + "snapshotId="
                + snapshotId
                + ", partition="
                + partition
                + ", bucket="
                + bucket
                + ", totalBuckets="
                + totalBuckets
                + ", beforeFiles="
                + beforeFiles
                + ", beforeDeletionFiles="
                + beforeDeletionFiles
                + ", afterFiles="
                + afterFiles
                + ", afterDeletionFiles="
                + afterDeletionFiles
                + ", isStreaming="
                + isStreaming
                + '}';
    }

    private void writeObject(ObjectOutputStream objectOutputStream) throws IOException {
        DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(objectOutputStream);
        out.writeInt(VERSION);
        out.writeLong(snapshotId);
        serializeBinaryRow(partition, out);
        out.writeInt(bucket);
        out.writeInt(totalBuckets);

        DataFileMetaSerializer dataFileSerializer = new DataFileMetaSerializer();
        out.writeInt(beforeFiles.size());
        for (DataFileMeta file : beforeFiles) {
            dataFileSerializer.serialize(file, out);
        }

        DeletionFile.serializeList(out, beforeDeletionFiles);

        out.writeInt(afterFiles.size());
        for (DataFileMeta file : afterFiles) {
            dataFileSerializer.serialize(file, out);
        }

        DeletionFile.serializeList(out, afterDeletionFiles);

        out.writeBoolean(isStreaming);
    }

    private void readObject(ObjectInputStream objectInputStream)
            throws IOException, ClassNotFoundException {
        DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(objectInputStream);
        int version = in.readInt();
        if (version != VERSION) {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }

        snapshotId = in.readLong();
        partition = deserializeBinaryRow(in);
        bucket = in.readInt();
        totalBuckets = in.readInt();

        DataFileMetaSerializer dataFileMetaSerializer = new DataFileMetaSerializer();
        FunctionWithIOException<DataInputView, DeletionFile> deletionFileSerializer =
                DeletionFile::deserialize;

        int beforeNumber = in.readInt();
        beforeFiles = new ArrayList<>(beforeNumber);
        for (int i = 0; i < beforeNumber; i++) {
            beforeFiles.add(dataFileMetaSerializer.deserialize(in));
        }

        beforeDeletionFiles = DeletionFile.deserializeList(in, deletionFileSerializer);

        int fileNumber = in.readInt();
        afterFiles = new ArrayList<>(fileNumber);
        for (int i = 0; i < fileNumber; i++) {
            afterFiles.add(dataFileMetaSerializer.deserialize(in));
        }

        afterDeletionFiles = DeletionFile.deserializeList(in, deletionFileSerializer);

        isStreaming = in.readBoolean();
    }
}
