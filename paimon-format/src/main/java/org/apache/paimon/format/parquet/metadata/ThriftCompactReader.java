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

    /**
     * Reusable result of a protocol header read. A single pooled instance serves both {@link
     * #readFieldHeader()} — where {@link #value} is the field id and {@link #type} the field type —
     * and {@link #readListHeader()} — where {@link #value} is the element count and {@link #type}
     * the element type. {@link #readFieldHeader()} returns {@code null} on {@link #STOP}.
     *
     * <p>Reuse: callers must copy {@link #value}/{@link #type} into locals before any further read,
     * because a nested read overwrites them in place. Keeping one pooled instance avoids allocating
     * one object per field/list header, which the selective footer parse hits constantly.
     */
    static final class Header {
        int value;
        byte type;
    }

    private final byte[] buf;
    private final int limit;
    private int pos;

    /** Field id stack for nested struct walks, mirroring TCompactProtocol's bookkeeping. */
    private int[] lastFieldIds = new int[16];

    private int structDepth;

    /** Pooled header result reused by {@link #readFieldHeader()} and {@link #readListHeader()}. */
    private final Header header = new Header();

    ThriftCompactReader(byte[] buf, int offset, int limit) {
        this.buf = buf;
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
     * Reads the next field header of the current struct into the pooled {@link Header}. Booleans
     * carry their value in the header itself, so the caller must inspect {@code header.type} for
     * {@link #BOOLEAN_TRUE}/{@link #BOOLEAN_FALSE} instead of reading a value byte.
     *
     * @return the pooled {@link Header} holding the field id ({@code header.value}) and field type
     *     ({@code header.type}), or {@code null} when the struct ended ({@link #STOP})
     */
    Header readFieldHeader() throws IOException {
        byte b = readByte();
        int type = b & 0x0f;
        if (type == STOP) {
            lastFieldIds[structDepth - 1] = 0;
            return null;
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
        header.value = fieldId;
        header.type = (byte) type;
        return header;
    }

    // ------------------------------------------------------------------ list / map

    /**
     * Reads a list/set header into the pooled {@link Header}: {@code header.value} is the element
     * count, {@code header.type} the element type. Sharing the pooled instance with {@link
     * #readFieldHeader()} keeps the parse allocation-free; the caller must copy both fields before
     * reading any element, since a nested header read overwrites them.
     */
    Header readListHeader() throws IOException {
        byte b = readByte();
        int size = (b & 0xf0) >>> 4;
        byte elemType = (byte) (b & 0x0f);
        if (size == 15) {
            size = (int) readVarint();
        }
        if (size < 0) {
            throw new IOException("malformed list size " + size + " at " + pos);
        }
        header.value = size;
        header.type = elemType;
        return header;
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
                skipBinary();
                return;
            case LIST:
            case SET:
                {
                    Header header = readListHeader();
                    skipElements(header.value, header.type);
                    return;
                }
            case MAP:
                skipMap();
                return;
            case STRUCT:
                structBegin();
                Header header;
                while ((header = readFieldHeader()) != null) {
                    skip(header.type);
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
            skipElement(elemType);
        }
    }

    /**
     * Skips {@code size} map entries. Compact protocol map entries are interleaved — each key is
     * immediately followed by its value — so they must be consumed in k1,v1,k2,v2 order; consuming
     * all keys before all values would leave the cursor mid-entry, and a trailing zero value would
     * then be mistaken for the enclosing struct's STOP.
     */
    private void skipMapEntries(long size, byte keyType, byte valueType) throws IOException {
        for (long i = 0; i < size; i++) {
            skipElement(keyType);
            skipElement(valueType);
        }
    }

    /**
     * Skips a map value: a varint size, then — only when non-empty — a single byte carrying the key
     * and value types in its two nibbles (high key, low value). The shared wire byte is decoded
     * here into separate named values so callers never see a fused representation.
     */
    private void skipMap() throws IOException {
        long size = readVarint();
        if (size > 0) {
            int kvTypes = readByte() & 0xff;
            byte keyType = (byte) (kvTypes >>> 4);
            byte valueType = (byte) (kvTypes & 0x0f);
            skipMapEntries(size, keyType, valueType);
        }
    }

    /** Skips a single element of the given type (list-element context). */
    private void skipElement(byte elemType) throws IOException {
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
                skipBinary();
                break;
            case LIST:
            case SET:
                {
                    Header header = readListHeader();
                    skipElements(header.value, header.type);
                    break;
                }
            case MAP:
                skipMap();
                break;
            case STRUCT:
                skip(STRUCT);
                break;
            default:
                throw new IOException(
                        "unknown thrift compact element type " + elemType + " at " + pos);
        }
    }

    private void skipBytes(int n) throws EOFException {
        // n < 0 must be rejected explicitly (a negative skip would move the cursor backwards),
        // and n > limit - pos (instead of pos + n > limit, which overflows for large n)
        if (n < 0 || n > limit - pos) {
            throw new EOFException("thrift compact reader overrun at " + pos);
        }
        pos += n;
    }

    /**
     * Skips a binary value. The unsigned varint length is validated in {@code long} space before
     * narrowing: a length that overflows {@code int} would otherwise wrap to a negative or small
     * value, and a crafted negative skip could move the cursor back onto the field header it just
     * read, looping the parse forever instead of failing.
     */
    private void skipBinary() throws IOException {
        long len = readVarint();
        if (len < 0 || len > Integer.MAX_VALUE || pos + len > limit) {
            throw new EOFException("malformed binary length " + len + " at " + pos);
        }
        pos += (int) len;
    }
}
