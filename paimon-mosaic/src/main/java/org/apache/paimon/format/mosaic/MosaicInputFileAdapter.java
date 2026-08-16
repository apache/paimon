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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.mosaic.InputFile;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

/**
 * Adapts Paimon's {@link FileIO} to Mosaic's {@link InputFile} interface.
 *
 * <p>Maintains a single {@link SeekableInputStream}. If the stream implements {@link
 * VectoredReadable}, reads use {@link VectoredReadable#preadFully} which is thread-safe. Otherwise,
 * reads are synchronized to protect seek+read sequences.
 */
public class MosaicInputFileAdapter implements InputFile, Closeable {

    private final Path path;
    private final SeekableInputStream in;
    private final VectoredReadable vectoredReadable;

    public MosaicInputFileAdapter(FileIO fileIO, Path path) throws IOException {
        this.path = path;
        this.in = fileIO.newInputStream(path);
        this.vectoredReadable = in instanceof VectoredReadable ? (VectoredReadable) in : null;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        if (vectoredReadable != null) {
            vectoredReadable.preadFully(position, buffer, offset, length);
        } else {
            synchronized (in) {
                in.seek(position);
                int remaining = length;
                int off = offset;
                while (remaining > 0) {
                    int read = in.read(buffer, off, remaining);
                    if (read < 0) {
                        throw new EOFException(
                                "Reached end of file while reading "
                                        + path
                                        + " at position "
                                        + position);
                    }
                    off += read;
                    remaining -= read;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
