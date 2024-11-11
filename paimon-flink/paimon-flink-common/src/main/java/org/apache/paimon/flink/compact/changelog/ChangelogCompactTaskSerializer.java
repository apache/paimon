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

package org.apache.paimon.flink.compact.changelog;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.CollectionUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Serializer for {@link ChangelogCompactTask}. */
public class ChangelogCompactTaskSerializer
        implements SimpleVersionedSerializer<ChangelogCompactTask> {
    private static final int CURRENT_VERSION = 1;

    private final DataFileMetaSerializer dataFileSerializer;

    public ChangelogCompactTaskSerializer() {
        this.dataFileSerializer = new DataFileMetaSerializer();
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(ChangelogCompactTask obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serialize(obj, view);
        return out.toByteArray();
    }

    @Override
    public ChangelogCompactTask deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        return deserialize(version, view);
    }

    private void serialize(ChangelogCompactTask task, DataOutputView view) throws IOException {
        view.writeLong(task.checkpointId());
        serializeBinaryRow(task.partition(), view);
        // serialize newFileChangelogFiles map
        serializeMap(task.newFileChangelogFiles(), view);
        serializeMap(task.compactChangelogFiles(), view);
    }

    private ChangelogCompactTask deserialize(int version, DataInputView view) throws IOException {
        if (version != getVersion()) {
            throw new RuntimeException("Can not deserialize version: " + version);
        }

        return new ChangelogCompactTask(
                view.readLong(),
                deserializeBinaryRow(view),
                deserializeMap(view),
                deserializeMap(view));
    }

    private void serializeMap(Map<Integer, List<DataFileMeta>> map, DataOutputView view)
            throws IOException {
        view.writeInt(map.size());
        for (Map.Entry<Integer, List<DataFileMeta>> entry : map.entrySet()) {
            view.writeInt(entry.getKey());
            if (entry.getValue() == null) {
                throw new IllegalArgumentException(
                        "serialize error. no value for bucket-" + entry.getKey());
            }
            dataFileSerializer.serializeList(entry.getValue(), view);
        }
    }

    private Map<Integer, List<DataFileMeta>> deserializeMap(DataInputView view) throws IOException {
        final int size = view.readInt();

        final Map<Integer, List<DataFileMeta>> map =
                CollectionUtil.newHashMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            map.put(view.readInt(), dataFileSerializer.deserializeList(view));
        }

        return map;
    }
}
