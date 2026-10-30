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

package org.apache.paimon.format.parquet.metadata;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Hand-written reader of the Thrift <b>Compact</b> protocol over an in-memory byte array.
 *
 * <p>It is the IO foundation of {@link SelectiveFooterParser}: parquet footers are serialized with
 * the compact protocol, whose variable-length encoding forces any parser to scan every byte, but
 * <i>scanning</i> and <i>materializing</i> are different costs. This reader exposes per-field
 * control to the caller: fields it does not need are consumed via {@link #skip(byte)} (zero
 * allocation).
 *
 * <p>Only the subset of the protocol used by parquet footers is implemented; unknown fields are
 * skip-consumed so the reader stays forward compatible with future parquet.thrift revisions.
 */
final class ThriftCompactReader {

    // Compact protocol element types.
    static final byte STOP = 0;
    static final byte BOOLEAN_TRUE = 1;
    static final byte BOOLEAN_FALSE = 2;
    static final byte BYTE = 3;
    static final byte I16 = 4;
    static final byte I32 = 5;
    static final byte I64 = 6;
    static final byte DOUBLE = 7;
    static final byte BINARY = 8;
    static final byte LIST = 9;
    static final byte SET = 10;
    static final byte MAP = 11;
    static final byte STRUCT = 12;

    private final byte[] buf;
    private final int offset;
    private final int limit;
    private int pos;

    /** Field id stack for nested struct walks, mirroring TCompactProtocol's bookkeeping. */
    private int[] lastFieldIds = new int[16];

    private int structDepth;

    ThriftCompactReader(byte[] buf, int offset, int limit) {
        this.buf = buf;
        this.offset = offset;
        this.limit = limit;
        this.pos = offset;
    }

    ThriftCompactReader(byte[] buf) {
        this(buf, 0, buf.length);
    }

    int position() {
        return pos;
    }

    // ------------------------------------------------------------------ primitives

    private byte readByte() throws EOFException {
        if (pos >= limit) {
            throw new EOFException("thrift compact reader overrun at " + pos);
        }
        return buf[pos++];
    }

    /** Reads a single raw byte (compact i8 value). */
    byte readByteValue() throws EOFException {
        return readByte();
    }

    void readBytes(byte[] target, int off, int len) throws IOException {
        if (pos + len > limit) {
            throw new EOFException("thrift compact reader overrun at " + pos);
        }
        System.arraycopy(buf, pos, target, off, len);
        pos += len;
    }

    /** Unsigned varint (up to 10 bytes for a long). */
    private long readVarint() throws IOException {
        long result = 0;
        int shift = 0;
        while (true) {
            byte b = readByte();
            result |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift >= 70) {
                throw new IOException("malformed varint in parquet footer");
            }
        }
    }

    private long zigzag(long v) {
        return (v >>> 1) ^ -(v & 1);
    }

    long readI64() throws IOException {
        return zigzag(readVarint());
    }

    int readI32() throws IOException {
        return (int) zigzag(readVarint());
    }

    short readI16() throws IOException {
        return (short) zigzag(readVarint());
    }

    byte[] readBinary() throws IOException {
        int len = (int) readVarint();
        if (len < 0 || pos + len > limit) {
            throw new EOFException("malformed binary length " + len + " at " + pos);
        }
        byte[] result = new byte[len];
        System.arraycopy(buf, pos, result, 0, len);
        pos += len;
        return result;
    }

    String readString() throws IOException {
        return new String(readBinary(), StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------ struct / field

    /** Enters a struct; must be paired with {@link #structEnd()} after the STOP field. */
    void structBegin() {
        if (structDepth == lastFieldIds.length) {
            int[] grown = new int[lastFieldIds.length * 2];
            System.arraycopy(lastFieldIds, 0, grown, 0, structDepth);
            lastFieldIds = grown;
        }
        lastFieldIds[structDepth++] = 0;
    }

    void structEnd() {
        structDepth--;
    }

    /**
     * Reads the next field header of the current struct. Booleans carry their value in the header
     * itself, so the caller must inspect {@code type} for {@link #BOOLEAN_TRUE}/{@link
     * #BOOLEAN_FALSE} instead of reading a value byte.
     *
     * @return {@code fieldId << 8 | type}; {@code type == STOP} means the struct ended
     */
    int readFieldHeader() throws IOException {
        byte b = readByte();
        int type = b & 0x0f;
        if (type == STOP) {
            lastFieldIds[structDepth - 1] = 0;
            return STOP;
        }
        int delta = (b & 0xf0) >>> 4;
        int fieldId;
        if (delta == 0) {
            // short field id, zigzag varint
            fieldId = (int) zigzag(readVarint());
        } else {
            fieldId = lastFieldIds[structDepth - 1] + delta;
        }
        lastFieldIds[structDepth - 1] = fieldId;
        return (fieldId << 8) | type;
    }

    static int fieldId(int fieldHeader) {
        return fieldHeader >>> 8;
    }

    static byte type(int fieldHeader) {
        return (byte) (fieldHeader & 0xff);
    }

    // ------------------------------------------------------------------ list / map

    /** @return {@code size << 8 | elemType} */
    int readListHeader() throws IOException {
        byte b = readByte();
        int size = (b & 0xf0) >>> 4;
        int elemType = b & 0x0f;
        if (size == 15) {
            size = (int) readVarint();
        }
        if (size < 0) {
            throw new IOException("malformed list size " + size + " at " + pos);
        }
        return (size << 8) | elemType;
    }

    static int listSize(int listHeader) {
        return listHeader >>> 8;
    }

    static byte listElemType(int listHeader) {
        return (byte) (listHeader & 0xff);
    }

    // ------------------------------------------------------------------ skipping

    /** Skips the value of a field of the given compact type (struct-member context). */
    void skip(byte type) throws IOException {
        switch (type) {
            case BOOLEAN_TRUE:
            case BOOLEAN_FALSE:
                return; // value is in the field header
            case BYTE:
                skipBytes(1);
                return;
            case I16:
            case I32:
            case I64:
                readVarint();
                return;
            case DOUBLE:
                skipBytes(8);
                return;
            case BINARY:
                skipBytes((int) readVarint());
                return;
            case LIST:
            case SET:
                {
                    int header = readListHeader();
                    skipElements(listSize(header), listElemType(header));
                    return;
                }
            case MAP:
                {
                    long size = readVarint();
                    if (size > 0) {
                        int kvTypes = readByte() & 0xff;
                        skipElements((int) size, (byte) (kvTypes >>> 4));
                        skipElements((int) size, (byte) (kvTypes & 0x0f));
                    }
                    return;
                }
            case STRUCT:
                structBegin();
                int header;
                while ((header = readFieldHeader()) != STOP) {
                    skip(type(header));
                }
                structEnd();
                return;
            default:
                throw new IOException("unknown thrift compact type " + type + " at " + pos);
        }
    }

    /** Skips {@code count} elements of the given type (list-element context). */
    private void skipElements(int count, byte elemType) throws IOException {
        for (int i = 0; i < count; i++) {
            switch (elemType) {
                case BOOLEAN_TRUE:
                case BOOLEAN_FALSE:
                    skipBytes(1); // booleans are real bytes inside lists
                    break;
                case BYTE:
                    skipBytes(1);
                    break;
                case I16:
                case I32:
                case I64:
                    readVarint();
                    break;
                case DOUBLE:
                    skipBytes(8);
                    break;
                case BINARY:
                    skipBytes((int) readVarint());
                    break;
                case LIST:
                case SET:
                    {
                        int header = readListHeader();
                        skipElements(listSize(header), listElemType(header));
                        break;
                    }
                case MAP:
                    {
                        long size = readVarint();
                        if (size > 0) {
                            int kvTypes = readByte() & 0xff;
                            skipElements((int) size, (byte) (kvTypes >>> 4));
                            skipElements((int) size, (byte) (kvTypes & 0x0f));
                        }
                        break;
                    }
                case STRUCT:
                    skip(STRUCT);
                    break;
                default:
                    throw new IOException(
                            "unknown thrift compact element type " + elemType + " at " + pos);
            }
        }
    }

    private void skipBytes(int n) throws EOFException {
        if (pos + n > limit) {
            throw new EOFException("thrift compact reader overrun at " + pos);
        }
        pos += n;
    }
}
