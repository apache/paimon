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

package org.apache.paimon.service.messages;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.service.network.messages.MessageBody;
import org.apache.paimon.service.network.messages.MessageDeserializer;

import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.paimon.service.network.messages.MessageDeserializer.readBytes;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** The response containing values sent by the Server to the Client. */
public class KvResponse extends MessageBody {

    private final BinaryRow[] values;

    public KvResponse(BinaryRow[] values) {
        this.values = values;
    }

    public BinaryRow[] values() {
        return values;
    }

    @Override
    public byte[] serialize() {
        // value size
        int size = 4;

        byte[][] valueBytesArray = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            BinaryRow value = values[i];
            // is null
            size += 1;
            if (value != null) {
                byte[] valueBytes = serializeBinaryRow(value);
                valueBytesArray[i] = valueBytes;
                size += 4 + valueBytes.length;
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(valueBytesArray.length);

        for (byte[] valueBytes : valueBytesArray) {
            if (valueBytes == null) {
                buffer.put((byte) 1);
            } else {
                buffer.put((byte) 0);
                buffer.putInt(valueBytes.length).put(valueBytes);
            }
        }

        return buffer.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KvResponse that = (KvResponse) o;
        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    /** A {@link MessageDeserializer deserializer} for {@link KvResponseDeserializer}. */
    public static class KvResponseDeserializer implements MessageDeserializer<KvResponse> {

        @Override
        public KvResponse deserializeMessage(ByteBuf buf) {
            int valueSize = buf.readInt();

            BinaryRow[] values = new BinaryRow[valueSize];
            for (int i = 0; i < valueSize; i++) {
                if (buf.readByte() != 1) {
                    values[i] = deserializeBinaryRow(readBytes(buf, buf.readInt()));
                }
            }

            return new KvResponse(values);
        }
    }
}
