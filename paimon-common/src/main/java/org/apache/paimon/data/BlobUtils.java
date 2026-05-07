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

    public static Blob fromBytes(
            byte[] bytes, @Nullable UriReaderFactory uriReaderFactory, @Nullable FileIO fileIO) {
        return fromBytes(bytes, uriReaderFactory, fileIO, true);
    }

    public static Blob fromBytes(
            byte[] bytes,
            @Nullable UriReaderFactory uriReaderFactory,
            @Nullable FileIO fileIO,
            boolean allowBlobData) {
        return fromBytes(
                bytes,
                uriReaderFactory == null ? null : uriReaderFactory::create,
                fileIO,
                allowBlobData);
    }

    public static Blob fromBytesWithReader(
            byte[] bytes, @Nullable UriReader uriReader, @Nullable FileIO fileIO) {
        return fromBytesWithReader(bytes, uriReader, fileIO, true);
    }

    public static Blob fromBytesWithReader(
            byte[] bytes,
            @Nullable UriReader uriReader,
            @Nullable FileIO fileIO,
            boolean allowBlobData) {
        return fromBytes(bytes, uri -> uriReader, fileIO, allowBlobData);
    }

    private static Blob fromBytes(
            byte[] bytes,
            @Nullable UriReaderCreator uriReaderCreator,
            @Nullable FileIO fileIO,
            boolean allowBlobData) {
        if (bytes == null) {
            return null;
        }

        if (BlobViewStruct.isBlobViewStruct(bytes)) {
            return Blob.fromView(BlobViewStruct.deserialize(bytes));
        }

        if (BlobDescriptor.isBlobDescriptor(bytes) || !allowBlobData) {
            BlobDescriptor descriptor = BlobDescriptor.deserialize(bytes);
            UriReader reader =
                    uriReaderCreator != null
                            ? uriReaderCreator.create(descriptor.uri())
                            : UriReader.fromFile(fileIO);
            return Blob.fromDescriptor(reader, descriptor);
        }

        return new BlobData(bytes);
    }

    public static byte[] serializeBlob(Blob blob) {
        if (blob instanceof BlobView) {
            return ((BlobView) blob).viewStruct().serialize();
        }
        return blob.toDescriptor().serialize();
    }

    private interface UriReaderCreator {

        UriReader create(String uri);
    }

    private BlobUtils() {}
}
