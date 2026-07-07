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

package org.apache.paimon.flink.sink.state;

import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.Map;

/** Versioned serializer for {@link CoordinatorState}. */
public class CoordinatorStateSerializer implements SimpleVersionedSerializer<CoordinatorState> {

    private static final int CURRENT_VERSION = 1;

    private final MapSerializer<String, byte[]> committerStateSerializer =
            new MapSerializer<>(StringSerializer.INSTANCE, BytePrimitiveArraySerializer.INSTANCE);

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(CoordinatorState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeUTF(state.getCommitUser());
        out.writeLong(state.getWatermark());
        committerStateSerializer.serialize(state.getCommitterStates(), out);
        return out.getCopyOfBuffer();
    }

    @Override
    public CoordinatorState deserialize(int version, byte[] serialized) throws IOException {
        Preconditions.checkState(
                version == CURRENT_VERSION,
                "Could not deserialize coordinator state of version "
                        + version
                        + ", expected version "
                        + CURRENT_VERSION);
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        String commitUser = in.readUTF();
        long watermark = in.readLong();
        Map<String, byte[]> committerStates = committerStateSerializer.deserialize(in);
        return new CoordinatorState(commitUser, watermark, committerStates);
    }
}
