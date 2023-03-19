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

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/** {@link SimpleVersionedSerializer} for {@link Committable}. */
public class CommittableSerializer implements SimpleVersionedSerializer<Committable> {

    private final CommitMessageSerializer commitMessageSerializer;

    public CommittableSerializer(CommitMessageSerializer commitMessageSerializer) {
        this.commitMessageSerializer = commitMessageSerializer;
    }

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(Committable committable) throws IOException {
        byte[] wrapped;
        int version;
        switch (committable.kind()) {
            case FILE:
                version = commitMessageSerializer.getVersion();
                wrapped =
                        commitMessageSerializer.serialize(
                                (CommitMessage) committable.wrappedCommittable());
                break;
            case LOG_OFFSET:
                version = 1;
                wrapped = ((LogOffsetCommittable) committable.wrappedCommittable()).toBytes();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported kind: " + committable.kind());
        }

        return ByteBuffer.allocate(8 + 1 + wrapped.length + 4)
                .putLong(committable.checkpointId())
                .put(committable.kind().toByteValue())
                .put(wrapped)
                .putInt(version)
                .array();
    }

    @Override
    public Committable deserialize(int committableVersion, byte[] bytes) throws IOException {
        if (committableVersion != getVersion()) {
            throw new RuntimeException("Can not deserialize version: " + committableVersion);
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long checkpointId = buffer.getLong();
        Committable.Kind kind = Committable.Kind.fromByteValue(buffer.get());
        byte[] wrapped = new byte[bytes.length - 13];
        buffer.get(wrapped);
        int version = buffer.getInt();

        Object wrappedCommittable;
        switch (kind) {
            case FILE:
                wrappedCommittable = commitMessageSerializer.deserialize(version, wrapped);
                break;
            case LOG_OFFSET:
                wrappedCommittable = LogOffsetCommittable.fromBytes(wrapped);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported kind: " + kind);
        }
        return new Committable(checkpointId, kind, wrappedCommittable);
    }
}
