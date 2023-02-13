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

package org.apache.flink.table.store.data.serializer;

import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.io.DataInputView;
import org.apache.flink.table.store.io.DataOutputView;
import org.apache.flink.table.store.memory.MemorySegmentUtils;

import java.io.IOException;

/** Serializer for {@link BinaryString}. */
public final class BinaryStringSerializer extends SerializerSingleton<BinaryString> {

    private static final long serialVersionUID = 1L;

    public static final BinaryStringSerializer INSTANCE = new BinaryStringSerializer();

    private BinaryStringSerializer() {}

    @Override
    public BinaryString copy(BinaryString from) {
        // BinaryString is the only implementation of BinaryString
        return from.copy();
    }

    @Override
    public void serialize(BinaryString string, DataOutputView target) throws IOException {
        target.writeInt(string.getSizeInBytes());
        MemorySegmentUtils.copyToView(
                string.getSegments(), string.getOffset(), string.getSizeInBytes(), target);
    }

    @Override
    public BinaryString deserialize(DataInputView source) throws IOException {
        return deserializeInternal(source);
    }

    public static BinaryString deserializeInternal(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return BinaryString.fromBytes(bytes);
    }
}
