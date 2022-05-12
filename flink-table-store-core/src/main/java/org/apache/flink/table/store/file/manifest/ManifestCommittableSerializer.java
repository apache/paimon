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

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileMetaSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.store.file.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.flink.table.store.file.utils.SerializationUtils.serializeBinaryRow;

/** {@link SimpleVersionedSerializer} for {@link ManifestCommittable}. */
public class ManifestCommittableSerializer
        implements SimpleVersionedSerializer<ManifestCommittable> {

    private final DataFileMetaSerializer dataFileSerializer;

    public ManifestCommittableSerializer() {
        this.dataFileSerializer = new DataFileMetaSerializer();
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(ManifestCommittable obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeUTF(obj.identifier());
        serializeOffsets(view, obj.logOffsets());
        serializeFiles(view, obj.newFiles());
        serializeFiles(view, obj.compactBefore());
        serializeFiles(view, obj.compactAfter());
        return out.toByteArray();
    }

    private void serializeOffsets(DataOutputViewStreamWrapper view, Map<Integer, Long> offsets)
            throws IOException {
        view.writeInt(offsets.size());
        for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
            view.writeInt(entry.getKey());
            view.writeLong(entry.getValue());
        }
    }

    private Map<Integer, Long> deserializeOffsets(DataInputDeserializer view) throws IOException {
        int size = view.readInt();
        Map<Integer, Long> offsets = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            offsets.put(view.readInt(), view.readLong());
        }
        return offsets;
    }

    private void serializeFiles(
            DataOutputViewStreamWrapper view,
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> files)
            throws IOException {
        view.writeInt(files.size());
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> entry : files.entrySet()) {
            serializeBinaryRow(entry.getKey(), view);
            view.writeInt(entry.getValue().size());
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry : entry.getValue().entrySet()) {
                view.writeInt(bucketEntry.getKey());
                view.writeInt(bucketEntry.getValue().size());
                for (DataFileMeta file : bucketEntry.getValue()) {
                    dataFileSerializer.serialize(file, view);
                }
            }
        }
    }

    private Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> deserializeFiles(
            DataInputDeserializer view) throws IOException {
        int partNumber = view.readInt();
        Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> files = new HashMap<>();
        for (int i = 0; i < partNumber; i++) {
            BinaryRowData part = deserializeBinaryRow(view);
            int bucketNumber = view.readInt();
            Map<Integer, List<DataFileMeta>> bucketMap = new HashMap<>();
            files.put(part, bucketMap);
            for (int j = 0; j < bucketNumber; j++) {
                int bucket = view.readInt();
                int fileNumber = view.readInt();
                List<DataFileMeta> fileMetas = new ArrayList<>();
                bucketMap.put(bucket, fileMetas);
                for (int k = 0; k < fileNumber; k++) {
                    fileMetas.add(dataFileSerializer.deserialize(view));
                }
            }
        }
        return files;
    }

    @Override
    public ManifestCommittable deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        return new ManifestCommittable(
                view.readUTF(),
                deserializeOffsets(view),
                deserializeFiles(view),
                deserializeFiles(view),
                deserializeFiles(view));
    }
}
