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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileMetaSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Manifest entries per partitioned with the corresponding snapshot id. */
public class PartitionedManifestMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The latest snapshot id seen at planning phase when manual compaction is triggered. */
    private final Long snapshotId;

    /** The manifest entries collected at planning phase when manual compaction is triggered. */
    private transient Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> manifestEntries;

    private final int partFieldCount;
    private final RowType keyType;
    private final RowType valueType;

    public PartitionedManifestMeta(
            Long snapshotId,
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> specifiedManifestEntries,
            int partFieldCount,
            RowType keyType,
            RowType valueType) {
        Preconditions.checkNotNull(snapshotId, "Specified snapshot should not be null.");
        Preconditions.checkNotNull(
                specifiedManifestEntries, "Specified manifest entries should not be null.");
        this.snapshotId = snapshotId;
        this.manifestEntries = specifiedManifestEntries;
        this.partFieldCount = partFieldCount;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public Long getSnapshotId() {
        return snapshotId;
    }

    public Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> getManifestEntries() {
        return manifestEntries;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        BinaryRowDataSerializer partSerializer = new BinaryRowDataSerializer(partFieldCount);
        DataFileMetaSerializer metaSerializer = new DataFileMetaSerializer(keyType, valueType);
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeInt(manifestEntries.size());
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> partEntry :
                manifestEntries.entrySet()) {
            partSerializer.serialize(partEntry.getKey(), view);
            Map<Integer, List<DataFileMeta>> bucketEntry = partEntry.getValue();
            view.writeInt(bucketEntry.size());
            for (Map.Entry<Integer, List<DataFileMeta>> entry : bucketEntry.entrySet()) {
                view.writeInt(entry.getKey());
                view.writeInt(entry.getValue().size());
                for (DataFileMeta meta : entry.getValue()) {
                    metaSerializer.serialize(meta, view);
                }
            }
        }
    }

    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        in.defaultReadObject();

        manifestEntries = new HashMap<>();
        BinaryRowDataSerializer partSerializer = new BinaryRowDataSerializer(partFieldCount);
        DataFileMetaSerializer metaSerializer = new DataFileMetaSerializer(keyType, valueType);
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
        int partitionNum = view.readInt();
        while (partitionNum > 0) {
            BinaryRowData partition = partSerializer.deserialize(view);
            Map<Integer, List<DataFileMeta>> bucketEntry = new HashMap<>();
            int bucketNum = view.readInt();
            while (bucketNum > 0) {
                int bucket = view.readInt();
                int entryNum = view.readInt();
                if (entryNum == 0) {
                    bucketEntry.put(bucket, Collections.emptyList());
                } else {
                    List<DataFileMeta> metas = new ArrayList<>();
                    while (entryNum > 0) {
                        metas.add(metaSerializer.deserialize(view));
                        entryNum--;
                    }
                    bucketEntry.put(bucket, metas);
                }
                bucketNum--;
            }
            manifestEntries.put(partition, bucketEntry);
            partitionNum--;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionedManifestMeta)) {
            return false;
        }
        PartitionedManifestMeta that = (PartitionedManifestMeta) o;
        return partFieldCount == that.partFieldCount
                && snapshotId.equals(that.snapshotId)
                && manifestEntries.equals(that.manifestEntries)
                && keyType.equals(that.keyType)
                && valueType.equals(that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, manifestEntries, partFieldCount, keyType, valueType);
    }
}
