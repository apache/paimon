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

package org.apache.paimon.elasticsearch.index.model;

import org.elasticsearch.vectorindex.model.VectorIndexMeta;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Structured metadata for an ES vector index file entry.
 *
 * <p>Wraps the file role (archive vs offset table) and the underlying {@link VectorIndexMeta}
 * serialized bytes into a single serializable unit. This replaces the previous approach of manually
 * prepending a raw role byte to the serialized VectorIndexMeta.
 *
 * <p>Binary format:
 *
 * <pre>
 *   [4 bytes] version (int, currently 1)
 *   [1 byte]  fileRole
 *   [4 bytes] vectorMetaLength (int)
 *   [N bytes] vectorMeta (serialized VectorIndexMeta)
 * </pre>
 */
public class ESIndexEntryMeta {

    private static final int CURRENT_VERSION = 1;

    /** Archive file containing all packed index segment files. */
    public static final byte FILE_ROLE_ARCHIVE = 0x01;

    /** Offset table file for random access into the archive. */
    public static final byte FILE_ROLE_OFFSETS = 0x02;

    private final byte fileRole;
    private final byte[] vectorMeta;

    public ESIndexEntryMeta(byte fileRole, byte[] vectorMeta) {
        this.fileRole = fileRole;
        this.vectorMeta = vectorMeta;
    }

    public byte fileRole() {
        return fileRole;
    }

    public byte[] vectorMeta() {
        return vectorMeta;
    }

    /** Deserializes the wrapped {@link VectorIndexMeta} from the stored bytes. */
    public VectorIndexMeta toVectorIndexMeta() throws IOException {
        return VectorIndexMeta.deserialize(vectorMeta);
    }

    /** Serializes this entry metadata to a byte array. */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        dataOutputStream.writeInt(CURRENT_VERSION);
        dataOutputStream.writeByte(fileRole);
        dataOutputStream.writeInt(vectorMeta.length);
        dataOutputStream.write(vectorMeta);
        dataOutputStream.flush();
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Deserializes an {@link ESIndexEntryMeta} from a byte array.
     *
     * <p>Supports two layouts:
     *
     * <ul>
     *   <li><b>v1 (current)</b>: {@code [4 bytes version=1] [1 byte fileRole] [4 bytes
     *       vectorMetaLength] [N bytes vectorMeta]}.
     *   <li><b>legacy (no version)</b>: {@code [1 byte fileRole] [N bytes vectorMeta]} — the
     *       original format before a version header was introduced. Detected when the first byte
     *       equals a known {@link #FILE_ROLE_ARCHIVE} / {@link #FILE_ROLE_OFFSETS} value.
     * </ul>
     */
    public static ESIndexEntryMeta deserialize(byte[] data) throws IOException {
        if (data.length == 0) {
            throw new IOException("Empty ESIndexEntryMeta payload");
        }
        // Legacy format: first byte is the file role; no version prefix.
        // The current format starts with a 4-byte version field whose first byte is 0x00 for
        // version=1 (and any small int), so a leading 0x01/0x02 disambiguates the legacy layout.
        byte first = data[0];
        if (first == FILE_ROLE_ARCHIVE || first == FILE_ROLE_OFFSETS) {
            byte[] vectorMeta = new byte[data.length - 1];
            System.arraycopy(data, 1, vectorMeta, 0, vectorMeta.length);
            return new ESIndexEntryMeta(first, vectorMeta);
        }

        DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(data));
        int version = dataInputStream.readInt();
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported ESIndexEntryMeta version: " + version);
        }
        byte fileRole = dataInputStream.readByte();
        int vectorMetaLength = dataInputStream.readInt();
        byte[] vectorMeta = new byte[vectorMetaLength];
        dataInputStream.readFully(vectorMeta);
        return new ESIndexEntryMeta(fileRole, vectorMeta);
    }
}
