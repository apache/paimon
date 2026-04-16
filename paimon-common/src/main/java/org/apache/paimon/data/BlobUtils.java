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

package org.apache.paimon.data;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.utils.UriReader;
import org.apache.paimon.utils.UriReaderFactory;

import javax.annotation.Nullable;

/** Utilities for decoding and encoding blob-related bytes. */
public class BlobUtils {

    /**
     * Decodes blob bytes for BLOB type fields. For BLOB_REF type, use {@link
     * DataGetters#getBlobRef(int)} instead.
     */
    public static Blob fromBytes(
            byte[] bytes, @Nullable UriReaderFactory uriReaderFactory, @Nullable FileIO fileIO) {
        if (bytes == null) {
            return null;
        }

        if (BlobDescriptor.isBlobDescriptor(bytes)) {
            BlobDescriptor descriptor = BlobDescriptor.deserialize(bytes);
            UriReader reader =
                    uriReaderFactory != null
                            ? uriReaderFactory.create(descriptor.uri())
                            : UriReader.fromFile(fileIO);
            return Blob.fromDescriptor(reader, descriptor);
        }

        return new BlobData(bytes);
    }

    public static byte[] serializeBlobReference(BlobRef blobRef) {
        return blobRef.reference().serialize();
    }

    private BlobUtils() {}
}
