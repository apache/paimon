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

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.BlobStream;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link BlobSerializer}. */
public class BlobSerializerTest extends SerializerTestBase<Blob> {

    @Override
    protected Serializer<Blob> createSerializer() {
        return BlobSerializer.INSTANCE;
    }

    @Override
    protected boolean deepEquals(Blob t1, Blob t2) {
        return t1.equals(t2);
    }

    @Override
    protected Blob[] getTestData() {
        return new Blob[] {
            new BlobData(new byte[] {1, 3, 1}),
            Blob.fromDescriptor(
                    uri -> {
                        throw new AssertionError("BlobRef serialization should not read data.");
                    },
                    new BlobDescriptor("file:/tmp/blob.bin", 12L, 34L))
        };
    }

    @Override
    protected Blob[] getSerializableTestData() {
        return new Blob[] {new BlobData(new byte[] {1, 3, 1})};
    }

    @Test
    public void testDeserializeBlobRefUsesThrowingUriReader() throws IOException {
        BlobDescriptor descriptor = new BlobDescriptor("file:/tmp/blob.bin", 3L, 7L);
        Blob deserialized =
                BlobSerializer.INSTANCE.deserializeFromBytes(
                        BlobSerializer.INSTANCE.serializeToBytes(
                                Blob.fromDescriptor(
                                        uri -> {
                                            throw new AssertionError(
                                                    "BlobRef serialization should not read data.");
                                        },
                                        descriptor)));

        assertThat(deserialized).isInstanceOf(BlobRef.class);
        assertThat(deserialized.toDescriptor()).isEqualTo(descriptor);
        assertThatThrownBy(deserialized::toData)
                .hasRootCauseInstanceOf(IOException.class)
                .hasMessageContaining("does not carry a UriReader");
    }

    @Test
    public void testBlobStreamIsUnsupported() {
        Blob blob = new BlobStream(() -> null);
        assertThatThrownBy(() -> BlobSerializer.INSTANCE.serializeToBytes(blob))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("BlobStream");
    }
}
