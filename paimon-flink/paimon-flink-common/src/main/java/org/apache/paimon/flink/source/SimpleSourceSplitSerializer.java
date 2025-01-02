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

package org.apache.paimon.flink.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** {@link SimpleVersionedSerializer} for {@link SimpleSourceSplit}. */
public class SimpleSourceSplitSerializer implements SimpleVersionedSerializer<SimpleSourceSplit> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(SimpleSourceSplit split) throws IOException {
        if (split.splitId() == null) {
            return new byte[0];
        }

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            writeString(out, split.splitId());
            writeString(out, split.value());
            return baos.toByteArray();
        }
    }

    @Override
    public SimpleSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (serialized.length == 0) {
            return new SimpleSourceSplit();
        }

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            String splitId = readString(in);
            String value = readString(in);
            return new SimpleSourceSplit(splitId, value);
        }
    }

    private void writeString(DataOutputStream out, String str) throws IOException {
        byte[] bytes = str.getBytes();
        out.writeInt(bytes.length);
        out.write(str.getBytes());
    }

    private String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes);
    }
}
