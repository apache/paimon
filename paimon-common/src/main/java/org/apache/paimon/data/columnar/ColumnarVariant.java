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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.data.variant.VariantCastArgs;
import org.apache.paimon.types.DataType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;

/** A zero-copy Variant view backed by value and metadata column vectors. */
public final class ColumnarVariant implements Variant {

    private final ByteBuffer value;
    private final ByteBuffer metadata;

    public ColumnarVariant(BytesColumnVector.Bytes value, BytesColumnVector.Bytes metadata) {
        this(buffer(value), buffer(metadata));
    }

    public ColumnarVariant(ByteBuffer value, ByteBuffer metadata) {
        this.value = value.slice().order(ByteOrder.LITTLE_ENDIAN);
        this.metadata = metadata.slice().order(ByteOrder.LITTLE_ENDIAN);
    }

    /** Creates a Variant view over the two binary fields in a columnar physical Variant row. */
    public ColumnarVariant(ColumnarRow row) {
        this(row.getBinaryBuffer(0), row.getBinaryBuffer(1));
    }

    @Override
    public byte[] metadata() {
        return bytes(metadata);
    }

    @Override
    public ByteBuffer metadataBuffer() {
        return metadata.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public byte[] value() {
        return bytes(value);
    }

    @Override
    public ByteBuffer valueBuffer() {
        return value.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    }

    private static ByteBuffer buffer(BytesColumnVector.Bytes bytes) {
        return ByteBuffer.wrap(bytes.data, bytes.offset, bytes.len)
                .slice()
                .order(ByteOrder.LITTLE_ENDIAN);
    }

    private static byte[] bytes(ByteBuffer buffer) {
        if (buffer.hasArray()
                && buffer.position() == 0
                && buffer.arrayOffset() == 0
                && buffer.remaining() == buffer.array().length) {
            return buffer.array();
        }
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    @Override
    public String toJson(ZoneId zoneId) {
        return materialize().toJson(zoneId);
    }

    @Override
    public Object variantGet(String path, DataType dataType, VariantCastArgs castArgs) {
        return materialize().variantGet(path, dataType, castArgs);
    }

    @Override
    public long sizeInBytes() {
        return (long) value.remaining() + metadata.remaining();
    }

    @Override
    public Variant copy() {
        return new GenericVariant(value(), metadata());
    }

    @Override
    public String toString() {
        return toJson();
    }

    private GenericVariant materialize() {
        return new GenericVariant(valueBuffer(), metadataBuffer());
    }
}
