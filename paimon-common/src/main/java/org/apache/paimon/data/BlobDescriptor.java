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

package org.apache.paimon.data;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Blob descriptor to describe a blob reference. */
public class BlobDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String uri;
    private final long offset;
    private final long length;

    public BlobDescriptor(String uri, long offset, long length) {
        this.uri = uri;
        this.offset = offset;
        this.length = length;
    }

    public String uri() {
        return uri;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlobDescriptor that = (BlobDescriptor) o;
        return offset == that.offset && length == that.length && Objects.equals(uri, that.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, offset, length);
    }

    @Override
    public String toString() {
        return "BlobDescriptor{"
                + "uri='"
                + uri
                + '\''
                + ", offset="
                + offset
                + ", length="
                + length
                + '}';
    }

    public byte[] serialize() {
        byte[] uriBytes = uri.getBytes(UTF_8);
        int uriLength = uriBytes.length;

        int totalSize = 4 + uriLength + 8 + 8;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        buffer.putInt(uriLength);
        buffer.put(uriBytes);

        buffer.putLong(offset);
        buffer.putLong(length);

        return buffer.array();
    }

    public static BlobDescriptor deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        int uriLength = buffer.getInt();
        byte[] uriBytes = new byte[uriLength];
        buffer.get(uriBytes);
        String uri = new String(uriBytes, StandardCharsets.UTF_8);

        long offset = buffer.getLong();
        long length = buffer.getLong();

        return new BlobDescriptor(uri, offset, length);
    }
}
