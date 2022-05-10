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

/** A serializer to serialize manifest entries per partition. */
public class PartitionedDataManifestSerializer implements Serializable {

    private static final long serialVersionUID = 1L;

    private final BinaryRowDataSerializer partSerializer;
    private final DataFileMetaSerializer metaSerializer;

    public PartitionedDataManifestSerializer(
            int partFieldCount, RowType keyType, RowType valueType) {
        this.partSerializer = new BinaryRowDataSerializer(partFieldCount);
        this.metaSerializer = new DataFileMetaSerializer(keyType, valueType);
    }

    public void serialize(
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> manifestEntries,
            ObjectOutputStream out)
            throws IOException {
        Preconditions.checkNotNull(
                manifestEntries,
                "Manifest entries for manual compaction to serialize should not be null.");
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

    public Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> deserialize(ObjectInputStream in)
            throws IOException {
        Preconditions.checkState(in.available() > 0, "No more available bytes to read in.");
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
        Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> manifestEntries = new HashMap<>();
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
        return manifestEntries;
    }
}
