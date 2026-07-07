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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableSerializer;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** {@link SimpleVersionedSerializer} for {@link CheckpointCommittables}. */
public class CheckpointCommittablesSerializer
        implements SimpleVersionedSerializer<CheckpointCommittables> {

    private final CommittableSerializer committableSerializer;

    public CheckpointCommittablesSerializer(CommittableSerializer committableSerializer) {
        this.committableSerializer = committableSerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(CheckpointCommittables value) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeLong(value.checkpointId());
        out.writeLong(value.watermark());
        // Nested serializer version comes before the list so the reader can pick the right decoder
        // before touching any list bytes — mirrors ManifestCommittableSerializer's layout.
        out.writeInt(committableSerializer.getVersion());
        List<Committable> committables = value.committables();
        out.writeInt(committables.size());
        for (Committable committable : committables) {
            byte[] wrapped = committableSerializer.serialize(committable);
            out.writeInt(wrapped.length);
            out.write(wrapped);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public CheckpointCommittables deserialize(int version, byte[] serialized) throws IOException {
        if (version != getVersion()) {
            throw new IOException("Unknown version " + version);
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        long checkpointId = in.readLong();
        long watermark = in.readLong();
        int committableVersion = in.readInt();
        int count = in.readInt();
        List<Committable> committables = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            committables.add(committableSerializer.deserialize(committableVersion, bytes));
        }
        return new CheckpointCommittables(checkpointId, committables, watermark);
    }
}
