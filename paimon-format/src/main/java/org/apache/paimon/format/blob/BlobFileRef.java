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
import org.apache.paimon.fs.OffsetSeekableInputStream;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import java.io.IOException;

/** BlobRef is a reference to a blob file. */
public class BlobFileRef implements Blob {

    private final FileIO fileIO;
    private final Path filePath;
    private final long blobLength;
    private final long blobOffset;

    public BlobFileRef(FileIO fileIO, Path filePath, long blobLength, long blobOffset) {
        this.fileIO = fileIO;
        this.filePath = filePath;
        this.blobLength = blobLength;
        this.blobOffset = blobOffset;
    }

    @Override
    public byte[] toBytes() {
        try {
            return IOUtils.readFully(newInputStream(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SeekableInputStream newInputStream() throws IOException {
        SeekableInputStream in = fileIO.newInputStream(filePath);
        // TODO validate Magic number?
        return new OffsetSeekableInputStream(in, blobOffset + 4, blobLength - 16);
    }
}
