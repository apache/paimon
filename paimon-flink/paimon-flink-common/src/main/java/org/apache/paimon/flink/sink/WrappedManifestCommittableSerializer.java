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

package org.apache.paimon.flink.sink;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestCommittableSerializer;
import org.apache.paimon.manifest.WrappedManifestCommittable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** {@link VersionedSerializer} for {@link ManifestCommittable}. */
public class WrappedManifestCommittableSerializer
        implements VersionedSerializer<WrappedManifestCommittable> {

    protected static final int CURRENT_VERSION = 1;

    private final ManifestCommittableSerializer manifestCommittableSerializer;

    public WrappedManifestCommittableSerializer() {
        this(new ManifestCommittableSerializer());
    }

    public WrappedManifestCommittableSerializer(
            ManifestCommittableSerializer manifestCommittableSerializer) {
        this.manifestCommittableSerializer = manifestCommittableSerializer;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(WrappedManifestCommittable wrapped) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

        view.writeLong(wrapped.checkpointId());
        view.writeLong(wrapped.watermark());

        // Serialize ManifestCommittable map inside WrappedManifestCommittable
        Map<Identifier, ManifestCommittable> map = wrapped.manifestCommittables();
        view.writeInt(map.size());
        for (Map.Entry<Identifier, ManifestCommittable> entry : map.entrySet()) {
            byte[] serializedKey = entry.getKey().getFullName().getBytes(StandardCharsets.UTF_8);
            byte[] serializedValue = manifestCommittableSerializer.serialize(entry.getValue());
            view.writeInt(serializedKey.length);
            view.write(serializedKey);
            view.writeInt(serializedValue.length);
            view.write(serializedValue);
        }
        return out.toByteArray();
    }

    @Override
    public WrappedManifestCommittable deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting WrappedManifestCommittable version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".\nWrappedManifestCommittable is not a compatible data structure. "
                            + "Please restart the job afresh (do not recover from savepoint).");
        }

        DataInputDeserializer view = new DataInputDeserializer(serialized);

        long checkpointId = view.readLong();
        long watermark = view.readLong();

        // Deserialize ManifestCommittable map inside WrappedManifestCommittable
        int mapSize = view.readInt();
        WrappedManifestCommittable wrappedManifestCommittable =
                new WrappedManifestCommittable(checkpointId, watermark);
        for (int i = 0; i < mapSize; i++) {
            int keyLength = view.readInt();
            byte[] serializedKey = new byte[keyLength];
            view.read(serializedKey);
            Identifier key =
                    Identifier.fromString(new String(serializedKey, StandardCharsets.UTF_8));
            int valueLength = view.readInt();
            byte[] serializedValue = new byte[valueLength];
            view.read(serializedValue);
            ManifestCommittable value =
                    manifestCommittableSerializer.deserialize(
                            manifestCommittableSerializer.getVersion(), serializedValue);
            wrappedManifestCommittable.putManifestCommittable(key, value);
        }

        return wrappedManifestCommittable;
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
