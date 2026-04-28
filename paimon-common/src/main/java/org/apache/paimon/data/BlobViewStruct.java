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
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Serialized metadata for a BLOB view field.
 *
 * <p>A blob view only stores the coordinates needed to locate the original blob value in the
 * upstream table: {@code tableName}, {@code fieldId} and {@code rowId}. The actual blob data is
 * resolved at read time by scanning the upstream table.
 */
public class BlobViewStruct implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final long MAGIC = 0x424C4F4256494557L; // "BLOBVIEW"
    private static final byte CURRENT_VERSION = 1;

    private final String tableName;
    private final int fieldId;
    private final long rowId;

    public BlobViewStruct(String tableName, int fieldId, long rowId) {
        this.tableName = tableName;
        this.fieldId = fieldId;
        this.rowId = rowId;
    }

    public String tableName() {
        return tableName;
    }

    public int fieldId() {
        return fieldId;
    }

    public long rowId() {
        return rowId;
    }

    public byte[] serialize() {
        byte[] tableBytes = tableName.getBytes(UTF_8);

        int totalSize = 1 + 8 + 4 + tableBytes.length + 4 + 8;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(CURRENT_VERSION);
        buffer.putLong(MAGIC);
        buffer.putInt(tableBytes.length);
        buffer.put(tableBytes);
        buffer.putInt(fieldId);
        buffer.putLong(rowId);
        return buffer.array();
    }

    public static BlobViewStruct deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        byte version = buffer.get();

        if (version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting BlobViewStruct version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }

        long magic = buffer.getLong();
        if (magic != MAGIC) {
            throw new IllegalArgumentException(
                    "Invalid BlobViewStruct: missing magic header. Expected magic: "
                            + MAGIC
                            + ", but found: "
                            + magic);
        }

        byte[] tableBytes = new byte[buffer.getInt()];
        buffer.get(tableBytes);

        int fieldId = buffer.getInt();
        long rowId = buffer.getLong();
        return new BlobViewStruct(new String(tableBytes, UTF_8), fieldId, rowId);
    }

    public static boolean isBlobViewStruct(byte[] bytes) {
        if (bytes == null || bytes.length < 9) {
            return false;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        byte version = buffer.get();
        return version == CURRENT_VERSION && MAGIC == buffer.getLong();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlobViewStruct that = (BlobViewStruct) o;
        return fieldId == that.fieldId
                && rowId == that.rowId
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, fieldId, rowId);
    }

    @Override
    public String toString() {
        return "BlobViewStruct{table="
                + tableName
                + ", fieldId="
                + fieldId
                + ", rowId="
                + rowId
                + "}";
    }
}
