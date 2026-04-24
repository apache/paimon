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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * A {@link Blob} that views a blob value stored in an upstream table.
 *
 * <p>The view is unresolved when it is read from a data file. It becomes readable after a {@link
 * BlobViewResolver} resolves the referenced descriptor.
 */
@Public
public class BlobView implements Blob, Serializable {

    private static final long serialVersionUID = 1L;

    private final BlobViewStruct viewStruct;
    @Nullable private UriReader uriReader;
    @Nullable private BlobDescriptor descriptor;

    public BlobView(BlobViewStruct viewStruct) {
        this.viewStruct = viewStruct;
    }

    public BlobViewStruct viewStruct() {
        return viewStruct;
    }

    public boolean isResolved() {
        return uriReader != null && descriptor != null;
    }

    /** Resolves this blob view in place by setting the reader and descriptor. */
    public void resolve(UriReader reader, BlobDescriptor desc) {
        this.uriReader = reader;
        this.descriptor = desc;
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
    public BlobDescriptor toDescriptor() {
        if (descriptor != null) {
            return descriptor;
        }
        throw new IllegalStateException("BlobView is not resolved.");
    }

    @Override
    public SeekableInputStream newInputStream() throws IOException {
        if (uriReader != null && descriptor != null) {
            return new OffsetSeekableInputStream(
                    uriReader.newInputStream(descriptor.uri()),
                    descriptor.offset(),
                    descriptor.length());
        }
        throw new IllegalStateException("BlobView is not resolved.");
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlobView blobView = (BlobView) o;
        return Objects.equals(viewStruct, blobView.viewStruct);
    }

    @Override
    public int hashCode() {
        return viewStruct.hashCode();
    }
}
