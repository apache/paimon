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
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/** {@link SimpleVersionedSerializer} for {@link Committable}. */
public class MultiTableCommittableSerializer implements SimpleVersionedSerializer<Committable> {

    private final CommitMessageSerializer commitMessageSerializer;
    private final CommittableSerializer committableSerializer;

    public MultiTableCommittableSerializer(CommitMessageSerializer commitMessageSerializer) {
        this.commitMessageSerializer = commitMessageSerializer;
        this.committableSerializer = new CommittableSerializer(commitMessageSerializer);
    }

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(Committable raw) throws IOException {
        MultiTableCommittable committable = (MultiTableCommittable) raw;

        String database = committable.getDatabase();
        int databaseLen = database.length();
        String table = committable.getTable();
        int tableLen = table.length();
        String commitUser = committable.getCommitUser();
        int commitUserLen = commitUser.length();

        int multiTableMetaLen = databaseLen + tableLen + commitUserLen + 3 * 4;

        byte[] serializedCommittable = committableSerializer.serialize(committable);

        return ByteBuffer.allocate(multiTableMetaLen + serializedCommittable.length)
                .putInt(databaseLen)
                .put(database.getBytes())
                .putInt(tableLen)
                .put(table.getBytes())
                .putInt(commitUserLen)
                .put(commitUser.getBytes())
                .put(serializedCommittable)
                .array();
    }

    @Override
    public Committable deserialize(int committableVersion, byte[] bytes) throws IOException {
        if (committableVersion != getVersion()) {
            throw new RuntimeException("Can not deserialize version: " + committableVersion);
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int databaseLen = buffer.getInt();
        byte[] databaseBytes = new byte[databaseLen];
        buffer.get(databaseBytes, 0, databaseLen);
        String database = new String(databaseBytes);
        int tableLen = buffer.getInt();
        byte[] tableBytes = new byte[tableLen];
        buffer.get(tableBytes, 0, tableLen);
        String table = new String(tableBytes);
        int commitUserLen = buffer.getInt();
        byte[] commitUserBytes = new byte[commitUserLen];
        buffer.get(commitUserBytes, 0, commitUserLen);
        String commitUser = new String(commitUserBytes);
        int multiTableMetaLen = databaseLen + tableLen + commitUserLen + 3 * 4;

        byte[] serializedCommittable = new byte[bytes.length - multiTableMetaLen];

        buffer.get(serializedCommittable, 0, bytes.length - multiTableMetaLen);
        Committable committable =
                committableSerializer.deserialize(committableVersion, serializedCommittable);

        return MultiTableCommittable.fromCommittable(
                Identifier.create(database, table), commitUser, committable);
    }
}
