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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.OffsetSeekableInputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.UriReader;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link Blob} refers blob in {@link BlobDescriptor}.
 *
 * @since 1.4.0
 */
@Public
public class BlobRef implements Blob {

    private final UriReader uriReader;
    private final BlobDescriptor descriptor;

    public BlobRef(UriReader uriReader, BlobDescriptor descriptor) {
        this.uriReader = uriReader;
        this.descriptor = descriptor;
    }

    @Override
    public byte[] toData() {
        try {
            return IOUtils.readFully(newInputStream(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SeekableInputStream newInputStream() throws IOException {
        return new OffsetSeekableInputStream(
                uriReader.newInputStream(descriptor.uri()),
                descriptor.offset(),
                descriptor.length());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlobRef blobRef = (BlobRef) o;
        return Objects.deepEquals(descriptor, blobRef.descriptor);
    }

    @Override
    public int hashCode() {
        return descriptor.hashCode();
    }
}
