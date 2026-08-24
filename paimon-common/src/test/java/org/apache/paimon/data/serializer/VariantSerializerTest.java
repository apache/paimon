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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.variant.BufferOnlyVariant;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link VariantSerializer}. */
public class VariantSerializerTest extends SerializerTestBase<Variant> {

    @Test
    public void testSerializeFromBuffers() throws IOException {
        GenericVariant expected = GenericVariant.fromJson("{\"age\":27,\"city\":\"Beijing\"}");
        DataOutputSerializer output = new DataOutputSerializer(32);

        VariantSerializer.INSTANCE.serialize(new BufferOnlyVariant(expected), output);
        Variant actual =
                VariantSerializer.INSTANCE.deserialize(
                        new DataInputDeserializer(output.getSharedBuffer(), 0, output.length()));

        assertThat(actual.toJson()).isEqualTo(expected.toJson());
    }

    @Test
    public void testLegacyVariantUsesBufferDefaults() throws IOException {
        GenericVariant expected = GenericVariant.fromJson("{\"age\":27}");
        DataOutputSerializer output = new DataOutputSerializer(32);

        VariantSerializer.INSTANCE.serialize(new LegacyVariant(expected), output);
        Variant actual =
                VariantSerializer.INSTANCE.deserialize(
                        new DataInputDeserializer(output.getSharedBuffer(), 0, output.length()));

        assertThat(actual).isEqualTo(expected);
    }

    @Override
    protected Serializer<Variant> createSerializer() {
        return VariantSerializer.INSTANCE;
    }

    @Override
    protected boolean deepEquals(Variant t1, Variant t2) {
        return t1.equals(t2);
    }

    @Override
    protected Variant[] getTestData() {
        return new Variant[] {GenericVariant.fromJson("{\"age\":27,\"city\":\"Beijing\"}")};
    }

    /** Mimics an implementation compiled against the original Variant interface. */
    private static class LegacyVariant implements Variant {

        private final Variant delegate;

        private LegacyVariant(Variant delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] metadata() {
            return delegate.metadata();
        }

        @Override
        public ByteBuffer metadataBuffer() {
            return delegate.metadataBuffer();
        }

        @Override
        public byte[] value() {
            return delegate.value();
        }

        @Override
        public ByteBuffer valueBuffer() {
            return delegate.valueBuffer();
        }

        @Override
        public String toJson(ZoneId zoneId) {
            return delegate.toJson(zoneId);
        }

        @Override
        public Object variantGet(
                String path,
                org.apache.paimon.types.DataType dataType,
                org.apache.paimon.data.variant.VariantCastArgs castArgs) {
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
    }
}
