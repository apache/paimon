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

package org.apache.paimon.data.variant;

import org.apache.paimon.types.DataType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;

/** A test Variant that exposes only non-zero-offset serialized buffer views. */
public class BufferOnlyVariant implements Variant {

    private static final int PREFIX_BYTES = 3;

    private final GenericVariant delegate;
    private final ByteBuffer value;
    private final ByteBuffer metadata;

    public BufferOnlyVariant(GenericVariant delegate) {
        this.delegate = delegate;
        this.value = withPadding(delegate.value());
        this.metadata = withPadding(delegate.metadata());
    }

    private static ByteBuffer withPadding(byte[] bytes) {
        ByteBuffer padded = ByteBuffer.allocateDirect(PREFIX_BYTES + bytes.length + 1);
        padded.position(PREFIX_BYTES);
        padded.put(bytes);
        return padded;
    }

    @Override
    public byte[] metadata() {
        throw new AssertionError("Serialized metadata should be consumed from metadataBuffer");
    }

    @Override
    public ByteBuffer metadataBuffer() {
        return buffer(metadata);
    }

    @Override
    public byte[] value() {
        throw new AssertionError("Serialized value should be consumed from valueBuffer");
    }

    @Override
    public ByteBuffer valueBuffer() {
        return buffer(value);
    }

    private static ByteBuffer buffer(ByteBuffer padded) {
        ByteBuffer view = padded.duplicate();
        view.position(PREFIX_BYTES);
        view.limit(padded.capacity() - 1);
        return view.slice().order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public String toJson(ZoneId zoneId) {
        return delegate.toJson(zoneId);
    }

    @Override
    public Object variantGet(String path, DataType dataType, VariantCastArgs castArgs) {
        return delegate.variantGet(path, dataType, castArgs);
    }

    @Override
    public long sizeInBytes() {
        return delegate.sizeInBytes();
    }

    @Override
    public Variant copy() {
        return delegate.copy();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
