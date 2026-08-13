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

package org.apache.avro.file;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/** Reusable compressed Avro block. */
public final class RawBlock {

    private DataFileStream.DataBlock block;
    private Codec codec;
    private Schema schema;
    private boolean decompressed;
    private ByteBuffer decompressedBuffer;

    RawBlock(DataFileStream.DataBlock block, Codec codec, Schema schema) {
        replace(block, codec, schema);
    }

    RawBlock replace(DataFileStream.DataBlock block, Codec codec, Schema schema) {
        this.block = block;
        this.codec = codec;
        this.schema = schema;
        this.decompressed = false;
        this.decompressedBuffer = null;
        return this;
    }

    DataFileStream.DataBlock dataBlock() {
        return block;
    }

    public long recordCount() {
        return block.getNumEntries();
    }

    public Schema schema() {
        return schema;
    }

    /** Returns an independently owned copy which remains valid after the reader advances. */
    public RawBlock stableCopy() {
        ByteBuffer source = block.getAsByteBuffer().duplicate();
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source);
        ((java.nio.Buffer) copy).flip();
        DataFileStream.DataBlock copiedBlock =
                new DataFileStream.DataBlock(copy, block.getNumEntries());
        copiedBlock.setFlushOnWrite(block.isFlushOnWrite());
        return new RawBlock(copiedBlock, codec, schema);
    }

    public ByteBuffer decompress(ByteBuffer reuse) throws IOException {
        if (!decompressed) {
            if (codec instanceof ZstandardCodec) {
                ByteBuffer source = block.getAsByteBuffer();
                ByteBuffer target =
                        reuse != null && reuse.hasArray() ? reuse : ByteBuffer.allocate(256 * 1024);
                int size = 0;
                try (InputStream compressed =
                                new ByteArrayInputStream(
                                        source.array(),
                                        source.arrayOffset() + source.position(),
                                        source.remaining());
                        InputStream input = ZstandardLoader.input(compressed, true)) {
                    while (true) {
                        if (size == target.capacity()) {
                            int grownCapacity =
                                    target.capacity() == 0
                                            ? 256 * 1024
                                            : Math.multiplyExact(target.capacity(), 2);
                            ByteBuffer grown = ByteBuffer.allocate(grownCapacity);
                            System.arraycopy(
                                    target.array(),
                                    target.arrayOffset(),
                                    grown.array(),
                                    grown.arrayOffset(),
                                    size);
                            target = grown;
                        }
                        int read =
                                input.read(
                                        target.array(),
                                        target.arrayOffset() + size,
                                        target.capacity() - size);
                        if (read < 0) {
                            break;
                        }
                        size += read;
                    }
                }
                ((java.nio.Buffer) target).position(0);
                ((java.nio.Buffer) target).limit(size);
                decompressedBuffer = target.duplicate();
            } else {
                decompressedBuffer = codec.decompress(block.getAsByteBuffer().duplicate());
            }
            decompressed = true;
        }
        return decompressedBuffer.duplicate();
    }

    /** Returns a single-block stream using a binary-compatible target schema. */
    public <D> DataFileStream<D> asStream(Schema targetSchema) throws IOException {
        return new SingleBlockStream<D>(targetSchema, codec, block);
    }

    private static final class SingleBlockStream<D> extends DataFileStream<D> {

        private final Schema schema;
        private final Codec codec;
        private DataBlock block;

        private SingleBlockStream(Schema schema, Codec codec, DataBlock block) throws IOException {
            super(new NoOpDatumReader<D>());
            this.schema = schema;
            this.codec = codec;
            this.block = block;
        }

        @Override
        public Schema getSchema() {
            return schema;
        }

        @Override
        Codec resolveCodec() {
            return codec;
        }

        @Override
        boolean hasNextBlock() {
            return block != null;
        }

        @Override
        DataBlock nextRawBlock(DataBlock reuse) {
            DataBlock result = block;
            block = null;
            return result;
        }

        @Override
        public void close() {}
    }

    private static final class NoOpDatumReader<D> implements DatumReader<D> {

        @Override
        public void setSchema(Schema schema) {}

        @Override
        public D read(D reuse, Decoder decoder) {
            throw new UnsupportedOperationException();
        }
    }
}
