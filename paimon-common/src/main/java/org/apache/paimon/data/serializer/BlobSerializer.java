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
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.utils.UriReader;

import java.io.IOException;

/** Type serializer for {@code Blob}. */
public class BlobSerializer extends SerializerSingleton<Blob> {

    private static final long serialVersionUID = 1L;
    private static final UriReader THROWING_URI_READER = new ThrowingUriReader();

    public static final BlobSerializer INSTANCE = new BlobSerializer();

    @Override
    public Blob copy(Blob from) {
        return from;
    }

    @Override
    public void serialize(Blob blob, DataOutputView target) throws IOException {
        BinarySerializer.INSTANCE.serialize(serializeBody(blob), target);
    }

    @Override
    public Blob deserialize(DataInputView source) throws IOException {
        byte[] bytes = BinarySerializer.INSTANCE.deserialize(source);
        return deserializeBody(bytes);
    }

    public byte[] serializeInternalBytes(Blob blob) {
        try {
            return serializeToBytes(blob);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize blob.", e);
        }
    }

    public Blob deserializeInternalBytes(byte[] bytes) {
        try {
            return deserializeFromBytes(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize blob.", e);
        }
    }

    private byte[] serializeBody(Blob blob) throws IOException {
        if (blob instanceof BlobData) {
            return ((BlobData) blob).toData();
        } else if (blob instanceof BlobRef) {
            return blob.toDescriptor().serialize();
        } else if (blob instanceof BlobStream) {
            throw new UnsupportedOperationException("BlobSerializer does not support BlobStream.");
        } else {
            throw new UnsupportedOperationException(
                    "BlobSerializer only supports BlobData and BlobRef, but found "
                            + blob.getClass().getSimpleName()
                            + ".");
        }
    }

    private Blob deserializeBody(byte[] bytes) throws IOException {
        if (BlobDescriptor.isBlobDescriptor(bytes)) {
            return Blob.fromDescriptor(THROWING_URI_READER, BlobDescriptor.deserialize(bytes));
        }
        return new BlobData(bytes);
    }

    private static class ThrowingUriReader implements UriReader {

        @Override
        public org.apache.paimon.fs.SeekableInputStream newInputStream(String uri)
                throws IOException {
            throw new IOException(
                    "BlobRef deserialized by BlobSerializer does not carry a UriReader. "
                            + "Use toDescriptor() instead. Descriptor URI: "
                            + uri);
        }
    }
}
