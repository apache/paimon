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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.globalindex.SortedIndexFileMeta;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.JsonSerdeUtil;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Manifest-level sorted metadata with the element type used by a Multivalue index. */
public final class MultiValueIndexFileMeta {

    // "MVIM". The trailer leaves the SortedIndexFileMeta prefix readable by older selectors.
    private static final int MAGIC = 0x4D56494D;
    private static final int TRAILER_SIZE = Integer.BYTES * 2;

    private MultiValueIndexFileMeta() {}

    public static byte[] serialize(SortedIndexFileMeta sortedMeta, DataType elementType) {
        byte[] sortedBytes = sortedMeta.serialize();
        byte[] typeBytes = typeSignature(elementType);
        ByteBuffer buffer =
                ByteBuffer.allocate(sortedBytes.length + typeBytes.length + TRAILER_SIZE);
        buffer.put(sortedBytes);
        buffer.put(typeBytes);
        buffer.putInt(typeBytes.length);
        buffer.putInt(MAGIC);
        return buffer.array();
    }

    public static boolean hasCompatibleElementType(
            @Nullable byte[] metadata, DataType elementType) {
        if (metadata == null || metadata.length < TRAILER_SIZE) {
            return false;
        }
        ByteBuffer trailer =
                ByteBuffer.wrap(metadata, metadata.length - TRAILER_SIZE, TRAILER_SIZE);
        int typeLength = trailer.getInt();
        int magic = trailer.getInt();
        if (magic != MAGIC || typeLength < 0 || typeLength > metadata.length - TRAILER_SIZE) {
            return false;
        }
        int typeOffset = metadata.length - TRAILER_SIZE - typeLength;
        return Arrays.equals(
                Arrays.copyOfRange(metadata, typeOffset, typeOffset + typeLength),
                typeSignature(elementType));
    }

    private static byte[] typeSignature(DataType elementType) {
        return JsonSerdeUtil.toJson(elementType).getBytes(StandardCharsets.UTF_8);
    }
}
