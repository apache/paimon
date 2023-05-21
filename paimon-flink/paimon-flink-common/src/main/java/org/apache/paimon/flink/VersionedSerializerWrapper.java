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

package org.apache.paimon.flink;

import org.apache.paimon.data.serializer.VersionedSerializer;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/** Wrap a {@link VersionedSerializer} to {@link SimpleVersionedSerializer}. */
public class VersionedSerializerWrapper<T> implements SimpleVersionedSerializer<T> {

    private final VersionedSerializer<T> serializer;

    public VersionedSerializerWrapper(VersionedSerializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public int getVersion() {
        return serializer.getVersion();
    }

    @Override
    public byte[] serialize(T obj) throws IOException {
        return serializer.serialize(obj);
    }

    @Override
    public T deserialize(int version, byte[] serialized) throws IOException {
        return serializer.deserialize(version, serialized);
    }
}
