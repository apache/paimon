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

package org.apache.paimon.format.avro;

import org.apache.paimon.types.DataType;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Decoder for sequentially reading records from Avro blocks without exposing Avro classes to
 * callers.
 */
public final class AvroRecordDecoder {

    private final Schema writerSchema;
    private final Schema recordSchema;
    private final int recordBranch;

    private @Nullable BinaryDecoder decoder;
    private @Nullable ByteBuffer borrowedView;
    private int blockOffset;
    private int blockLength;

    AvroRecordDecoder(Schema writerSchema) {
        this.writerSchema = writerSchema;
        if (writerSchema.getType() == Schema.Type.UNION) {
            int branchIndex = -1;
            Schema record = null;
            for (int i = 0; i < writerSchema.getTypes().size(); i++) {
                Schema branch = writerSchema.getTypes().get(i);
                if (branch.getType() == Schema.Type.RECORD) {
                    if (record != null) {
                        throw new IllegalArgumentException(
                                "Avro union contains multiple record branches.");
                    }
                    record = branch;
                    branchIndex = i;
                }
            }
            if (record == null) {
                throw new IllegalArgumentException("Avro schema is not a record or record union.");
            }
            this.recordSchema = record;
            this.recordBranch = branchIndex;
        } else if (writerSchema.getType() == Schema.Type.RECORD) {
            this.recordSchema = writerSchema;
            this.recordBranch = -1;
        } else {
            throw new IllegalArgumentException("Avro schema is not a record or record union.");
        }
    }

    /** Creates an independent decoder with the same writer schema and no current block. */
    public AvroRecordDecoder copy() {
        return new AvroRecordDecoder(writerSchema);
    }

    /** Returns the number of fields in the writer record. */
    public int fieldCount() {
        return recordSchema.getFields().size();
    }

    /** Returns the writer field name at the given position. */
    public String fieldName(int position) {
        return recordSchema.getFields().get(position).name();
    }

    /** Returns the Avro type of the writer field at the given position. */
    public FieldType fieldType(int position) {
        return FieldType.valueOf(recordSchema.getFields().get(position).schema().getType().name());
    }

    /** Creates a decoder for one writer field. */
    public FieldDecoder createFieldDecoder(int position, @Nullable DataType readType) {
        FieldReader reader =
                new FieldReaderFactory()
                        .visit(recordSchema.getFields().get(position).schema(), readType);
        return new FieldDecoder(reader);
    }

    /** Reuses this decoder for another block. */
    public void reset(byte[] bytes, int offset, int length) {
        decoder = DecoderFactory.get().binaryDecoder(bytes, offset, length, decoder);
        blockOffset = offset;
        blockLength = length;
        if (borrowedView == null || borrowedView.array() != bytes) {
            borrowedView = ByteBuffer.wrap(bytes);
        }
    }

    /** Returns whether a block has been supplied through {@link #reset(byte[], int, int)}. */
    public boolean isInitialized() {
        return decoder != null;
    }

    /** Returns whether the current block has been consumed. */
    public boolean isEnd() throws IOException {
        return decoder().isEnd();
    }

    /** Reads the union branch, if present, and returns whether it is the record branch. */
    public boolean readRecordStart() throws IOException {
        return recordBranch < 0 || decoder().readIndex() == recordBranch;
    }

    public int readInt() throws IOException {
        return decoder().readInt();
    }

    /** Returns the byte position relative to the beginning of the current block. */
    private int position() throws IOException {
        return blockLength - decoder().inputStream().available();
    }

    /** Returns the absolute byte position in the current block's backing array. */
    public int absolutePosition() throws IOException {
        return blockOffset + position();
    }

    /** Returns a borrowed view of an absolute range in the current block's backing array. */
    public ByteBuffer borrowedView(int start, int end) {
        if (borrowedView == null) {
            throw new IllegalStateException("No Avro block has been supplied.");
        }
        int blockEnd = blockOffset + blockLength;
        if (start < blockOffset || end < start || end > blockEnd) {
            throw new IllegalArgumentException(
                    String.format(
                            "Borrowed Avro byte range [%s, %s) is outside block range [%s, %s).",
                            start, end, blockOffset, blockEnd));
        }
        borrowedView.clear();
        borrowedView.position(start);
        borrowedView.limit(end);
        return borrowedView;
    }

    public byte[] readBytes() throws IOException {
        return decoder().readBytes(null).array();
    }

    public void skipBytes() throws IOException {
        decoder().skipBytes();
    }

    private BinaryDecoder decoder() {
        if (decoder == null) {
            throw new IllegalStateException("No Avro block has been supplied.");
        }
        return decoder;
    }

    /** Avro schema types represented without exposing Avro's {@link Schema.Type}. */
    public enum FieldType {
        RECORD,
        ENUM,
        ARRAY,
        MAP,
        UNION,
        FIXED,
        STRING,
        BYTES,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        NULL
    }

    /** Decoder for one field in the writer record. */
    public static final class FieldDecoder {

        private final FieldReader reader;

        private FieldDecoder(FieldReader reader) {
            this.reader = reader;
        }

        public Object read(AvroRecordDecoder decoder, @Nullable Object reuse) throws IOException {
            return reader.read(decoder.decoder(), reuse);
        }

        public void skip(AvroRecordDecoder decoder) throws IOException {
            reader.skip(decoder.decoder());
        }
    }
}
