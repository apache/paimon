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

package org.apache.paimon.format.blob;

import org.apache.paimon.data.Blob;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

/** Common reader functionality shared by blob element formats. */
abstract class AbstractBlobElementReader implements BlobElementSerializer.Reader {

    private final FileIO fileIO;
    private final String filePath;
    private final @Nullable SeekableInputStream in;
    private final boolean blobAsDescriptor;

    AbstractBlobElementReader(
            FileIO fileIO,
            Path filePath,
            @Nullable SeekableInputStream in,
            boolean blobAsDescriptor) {
        this.fileIO = fileIO;
        this.filePath = filePath.toString();
        this.in = in;
        this.blobAsDescriptor = blobAsDescriptor;
    }

    protected final @Nullable SeekableInputStream inputStream() {
        return in;
    }

    protected final boolean blobAsDescriptor() {
        return blobAsDescriptor;
    }

    protected final Blob readBlob(long position, long length) {
        if (in != null && !blobAsDescriptor) {
            return Blob.fromData(readInlineBlob(position, length));
        }
        return Blob.fromFile(fileIO, filePath, position, length);
    }

    protected final byte[] readInlineBlob(long position, long length) {
        byte[] blobData = new byte[(int) length];
        try {
            in.seek(position);
            IOUtils.readFully(in, blobData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return blobData;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(in);
    }
}
