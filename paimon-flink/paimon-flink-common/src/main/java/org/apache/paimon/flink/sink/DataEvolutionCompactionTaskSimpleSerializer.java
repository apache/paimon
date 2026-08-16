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

import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTaskSerializer;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/** {@link SimpleVersionedSerializer} for {@link DataEvolutionCompactTask}. */
public class DataEvolutionCompactionTaskSimpleSerializer
        implements SimpleVersionedSerializer<DataEvolutionCompactTask> {

    private final DataEvolutionCompactTaskSerializer compactionTaskSerializer;

    public DataEvolutionCompactionTaskSimpleSerializer(
            DataEvolutionCompactTaskSerializer compactionTaskSerializer) {
        this.compactionTaskSerializer = compactionTaskSerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DataEvolutionCompactTask compactionTask) throws IOException {
        byte[] wrapped = compactionTaskSerializer.serialize(compactionTask);
        int version = compactionTaskSerializer.getVersion();

        return ByteBuffer.allocate(wrapped.length + 4).put(wrapped).putInt(version).array();
    }

    @Override
    public DataEvolutionCompactTask deserialize(int compactionTaskVersion, byte[] bytes)
            throws IOException {
        if (compactionTaskVersion != getVersion()) {
            throw new RuntimeException("Can not deserialize version: " + compactionTaskVersion);
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] wrapped = new byte[bytes.length - 4];
        buffer.get(wrapped);
        int version = buffer.getInt();
        return compactionTaskSerializer.deserialize(version, wrapped);
    }
}
