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

import org.apache.paimon.data.serializer.VersionedSerializer;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** pwc state serializer using VersionedSerializer. */
public class PwcStateSerializer<GlobalCommitT> {

    private final VersionedSerializer<GlobalCommitT> globalCommitSerializer;

    public PwcStateSerializer(VersionedSerializer<GlobalCommitT> globalCommitSerializer) {
        this.globalCommitSerializer = globalCommitSerializer;
    }

    public byte[] serialize(PwcState<GlobalCommitT> state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputViewStreamWrapper(baos)) {

            out.writeLong(state.checkpointId);
            out.writeInt(globalCommitSerializer.getVersion());
            byte[] serialized = globalCommitSerializer.serialize(state.globalCommit);
            out.writeInt(serialized.length);
            out.write(serialized);

            out.flush();
            return baos.toByteArray();
        }
    }

    public PwcState<GlobalCommitT> deserialize(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputViewStreamWrapper(bais)) {

            long checkpointId = in.readLong();
            int version = in.readInt();

            int len = in.readInt();
            byte[] serialized = new byte[len];
            in.readFully(serialized);

            GlobalCommitT globalCommit = globalCommitSerializer.deserialize(version, serialized);

            return new PwcState<>(checkpointId, globalCommit);
        }
    }
}
