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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** {@link SimpleVersionedSerializer} for {@link GlobalCommittable}. */
public class GlobalCommittableSerializer<LogCommT>
        implements SimpleVersionedSerializer<GlobalCommittable<LogCommT>> {

    private final SimpleVersionedSerializer<LogCommT> logSerializer;

    private final ManifestCommittableSerializer fileSerializer;

    public GlobalCommittableSerializer(
            SimpleVersionedSerializer<LogCommT> logSerializer,
            ManifestCommittableSerializer fileSerializer) {
        this.logSerializer = logSerializer;
        this.fileSerializer = fileSerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(GlobalCommittable<LogCommT> committable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

        view.writeInt(logSerializer.getVersion());
        view.writeInt(committable.logCommittables().size());
        for (LogCommT commT : committable.logCommittables()) {
            byte[] bytes = logSerializer.serialize(commT);
            view.writeInt(bytes.length);
            view.write(bytes);
        }

        view.writeInt(fileSerializer.getVersion());
        byte[] bytes = fileSerializer.serialize(committable.fileCommittable());
        view.writeInt(bytes.length);
        view.write(bytes);

        return out.toByteArray();
    }

    @Override
    public GlobalCommittable<LogCommT> deserialize(int version, byte[] serialized)
            throws IOException {
        DataInputDeserializer view = new DataInputDeserializer(serialized);

        int logVersion = view.readInt();
        int logSize = view.readInt();
        List<LogCommT> logCommTList = new ArrayList<>();
        for (int i = 0; i < logSize; i++) {
            byte[] bytes = new byte[view.readInt()];
            view.read(bytes);
            logCommTList.add(logSerializer.deserialize(logVersion, bytes));
        }

        int fileVersion = view.readInt();
        byte[] bytes = new byte[view.readInt()];
        view.read(bytes);
        ManifestCommittable file = fileSerializer.deserialize(fileVersion, bytes);

        return new GlobalCommittable<>(logCommTList, file);
    }
}
