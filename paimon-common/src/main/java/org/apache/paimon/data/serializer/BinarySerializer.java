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

package org.apache.paimon.data.serializer;

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/** Type serializer for {@code byte[]}. */
public final class BinarySerializer extends SerializerSingleton<byte[]> {

    private static final long serialVersionUID = 1L;
    private static final int COPY_BUFFER_SIZE = 8192;

    /** Sharable instance of the IntSerializer. */
    public static final BinarySerializer INSTANCE = new BinarySerializer();

    @Override
    public byte[] copy(byte[] from) {
        byte[] copy = new byte[from.length];
        System.arraycopy(from, 0, copy, 0, from.length);
        return copy;
    }

    @Override
    public void serialize(byte[] record, DataOutputView target) throws IOException {
        encodeInt(target, record.length);
        target.write(record);
    }

    /**
     * Serializes the bytes between the buffer's position and limit without changing its position.
     */
    public void serialize(ByteBuffer record, DataOutputView target) throws IOException {
        int length = record.remaining();
        encodeInt(target, length);
        if (record.hasArray()) {
            target.write(record.array(), record.arrayOffset() + record.position(), length);
            return;
        }

        ByteBuffer duplicate = record.duplicate();
        byte[] copyBuffer = new byte[Math.min(length, COPY_BUFFER_SIZE)];
        while (duplicate.hasRemaining()) {
            int bytesToCopy = Math.min(duplicate.remaining(), copyBuffer.length);
            duplicate.get(copyBuffer, 0, bytesToCopy);
            target.write(copyBuffer, 0, bytesToCopy);
        }
    }

    @Override
    public byte[] deserialize(DataInputView source) throws IOException {
        int len = decodeInt(source);
        byte[] result = new byte[len];
        source.readFully(result);
        return result;
    }
}
