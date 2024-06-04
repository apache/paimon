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

package org.apache.paimon.manifest;

import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link VersionedSerializer} for {@link ManifestCommittable}. */
public class ManifestCommittableSerializer implements VersionedSerializer<ManifestCommittable> {

    private static final int CURRENT_VERSION = 3;

    private final CommitMessageSerializer commitMessageSerializer;

    public ManifestCommittableSerializer() {
        this.commitMessageSerializer = new CommitMessageSerializer();
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(ManifestCommittable obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeLong(obj.identifier());
        Long watermark = obj.watermark();
        if (watermark == null) {
            view.writeBoolean(true);
        } else {
            view.writeBoolean(false);
            view.writeLong(watermark);
        }
        serializeOffsets(view, obj.logOffsets());
        view.writeInt(commitMessageSerializer.getVersion());
        commitMessageSerializer.serializeList(obj.fileCommittables(), view);
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

    @Override
    public ManifestCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version > CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting ManifestCommittableSerializer version to be smaller or equal than "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }

        DataInputDeserializer view = new DataInputDeserializer(serialized);
        long identifier = view.readLong();
        Long watermark = view.readBoolean() ? null : view.readLong();
        Map<Integer, Long> offsets = deserializeOffsets(view);
        int fileCommittableSerializerVersion = view.readInt();
        List<CommitMessage> fileCommittables =
                deserializeCommitMessage(fileCommittableSerializerVersion, view);
        return new ManifestCommittable(identifier, watermark, offsets, fileCommittables);
    }

    protected List<CommitMessage> deserializeCommitMessage(int version, DataInputView view)
            throws IOException {
        return commitMessageSerializer.deserializeList(version, view);
    }

    private Map<Integer, Long> deserializeOffsets(DataInputDeserializer view) throws IOException {
        int size = view.readInt();
        Map<Integer, Long> offsets = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            offsets.put(view.readInt(), view.readLong());
        }
        return offsets;
    }
}
