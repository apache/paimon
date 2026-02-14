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

/**
 * Blob descriptor to describe a blob reference.
 *
 * <p>Memory Layout Description: All multi-byte numerical values (int/long) are stored using Little
 * Endian byte order.
 *
 * <pre>
 * | Offset (Bytes) | Field Name    | Type      | Size (Bytes) | Description                                         |
 * |----------------|---------------|-----------|--------------|-----------------------------------------------------|
 * | 0              | version       | byte      | 1            | Serialization structure version                     |
 * | 1              | uriLength     | int       | 4            | Length (N) of the URI string in UTF-8 bytes         |
 * | 5              | uriBytes      | byte[N]   | N            | UTF-8 encoded bytes of the URI string               |
 * | 5 + N          | offset        | long      | 8            | Starting offset of the Blob within the URI resource |
 * | 13 + N         | length        | long      | 8            | Length of the Blob data                             |
 * </pre>
 */
public class BlobDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final long MAGIC = 0x424C4F4244455343L; // "BLOBDESC"
    private static final byte CURRENT_VERSION = 2;

    private final byte version;
    private final String uri;
    private final long offset;
    private final long length;

    public BlobDescriptor(String uri, long offset, long length) {
        this(CURRENT_VERSION, uri, offset, length);
    }

    private BlobDescriptor(byte version, String uri, long offset, long length) {
        this.version = version;
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
        return version == that.version
                && offset == that.offset
                && length == that.length
                && Objects.equals(uri, that.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, uri, offset, length);
    }

    @Override
    public String toString() {
        return "BlobDescriptor{"
                + "version="
                + version
                + '\''
                + ", uri='"
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

        int totalSize = 1 + 8 + 4 + uriLength + 8 + 8;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        buffer.put(version);
        buffer.putLong(MAGIC);
        buffer.putInt(uriLength);
        buffer.put(uriBytes);

        buffer.putLong(offset);
        buffer.putLong(length);

        return buffer.array();
    }

    public static BlobDescriptor deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        byte version = buffer.get();
        if (version > CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting BlobDescriptor version to be less than or equal to "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }

        if (version > 1) {
            if (MAGIC != buffer.getLong()) {
                throw new IllegalArgumentException(
                        "Invalid BlobDescriptor: missing magic header. Expected magic: "
                                + MAGIC
                                + ", but found: "
                                + buffer.getLong());
            }
        }

        int uriLength = buffer.getInt();
        byte[] uriBytes = new byte[uriLength];
        buffer.get(uriBytes);
        String uri = new String(uriBytes, StandardCharsets.UTF_8);

        long offset = buffer.getLong();
        long length = buffer.getLong();
        return new BlobDescriptor(version, uri, offset, length);
    }

    public static boolean isBlobDescriptor(byte[] bytes) {
        if (bytes.length < 9) {
            return false;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        byte version = buffer.get();
        if (version == 1) {
            return true;
        } else if (version > CURRENT_VERSION) {
            return false;
        } else {
            return MAGIC == buffer.getLong();
        }
    }
}
