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
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * {@link SimpleVersionedSerializer} for {@link MultiTableCommittable}. If a type info class is
 * using this this serde of MultiTableCommittable. It should make sure that the operator that
 * produces it will include the database, table, and commit user information. This can be done by
 * calling MultiTableCommittable::fromCommittable.
 */
public class MultiTableCommittableSerializer
        implements SimpleVersionedSerializer<MultiTableCommittable> {

    private final CommittableSerializer committableSerializer;
    private final CommitMessageSerializer commitMessageSerializer;

    public MultiTableCommittableSerializer(CommitMessageSerializer commitMessageSerializer) {
        this.committableSerializer = new CommittableSerializer(commitMessageSerializer);
        this.commitMessageSerializer = commitMessageSerializer;
    }

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(MultiTableCommittable committable) throws IOException {
        // first serialize all metadata
        String database = committable.getDatabase();
        int databaseLen = database.getBytes(StandardCharsets.UTF_8).length;
        String table = committable.getTable();
        int tableLen = table.getBytes(StandardCharsets.UTF_8).length;

        int multiTableMetaLen = databaseLen + tableLen + 2 * 4;

        // use committable serializer (of the same version) to serialize committable
        byte[] serializedCommittable = serializeCommittable(committable);

        return ByteBuffer.allocate(multiTableMetaLen + serializedCommittable.length)
                .putInt(databaseLen)
                .put(database.getBytes())
                .putInt(tableLen)
                .put(table.getBytes())
                .put(serializedCommittable)
                .array();
    }

    @Override
    public MultiTableCommittable deserialize(int committableVersion, byte[] bytes)
            throws IOException {
        if (committableVersion != getVersion()) {
            throw new RuntimeException("Can not deserialize version: " + committableVersion);
        }

        // first deserialize all metadata
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int databaseLen = buffer.getInt();
        byte[] databaseBytes = new byte[databaseLen];
        buffer.get(databaseBytes, 0, databaseLen);
        String database = new String(databaseBytes, StandardCharsets.UTF_8);

        int tableLen = buffer.getInt();
        byte[] tableBytes = new byte[tableLen];
        buffer.get(tableBytes, 0, tableLen);
        String table = new String(tableBytes, StandardCharsets.UTF_8);
        int multiTableMetaLen = 4 + databaseLen + 4 + tableLen;

        // use committable serializer (of the same version) to deserialize committable
        byte[] serializedCommittable = new byte[bytes.length - multiTableMetaLen];

        buffer.get(serializedCommittable, 0, serializedCommittable.length);
        Committable committable = deserializeCommittable(committableVersion, serializedCommittable);

        return MultiTableCommittable.fromCommittable(
                Identifier.create(database, table), committable);
    }

    public byte[] serializeCommittable(MultiTableCommittable committable) throws IOException {
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

    public Committable deserializeCommittable(int committableVersion, byte[] bytes)
            throws IOException {
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
