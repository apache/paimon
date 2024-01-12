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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegmentUtils;

import java.io.IOException;

import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

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
        encodeInt(target, string.getSizeInBytes());
        MemorySegmentUtils.copyToView(
                string.getSegments(), string.getOffset(), string.getSizeInBytes(), target);
    }

    @Override
    public BinaryString deserialize(DataInputView source) throws IOException {
        return deserializeInternal(source);
    }

    public static BinaryString deserializeInternal(DataInputView source) throws IOException {
        int length = decodeInt(source);
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return BinaryString.fromBytes(bytes);
    }

    @Override
    public String serializeToString(BinaryString record) {
        return record.toString();
    }

    @Override
    public BinaryString deserializeFromString(String s) {
        return BinaryString.fromString(s);
    }
}
