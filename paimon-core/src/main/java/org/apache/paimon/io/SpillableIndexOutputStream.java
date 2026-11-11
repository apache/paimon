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

package org.apache.paimon.io;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** Keeps a small file index in memory and switches to an independent file at the threshold. */
public final class SpillableIndexOutputStream extends OutputStream {

    private final FileIO fileIO;
    private final Path path;
    private final int threshold;
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private OutputStream fileOutput;
    private boolean fileCreated;
    private boolean closed;

    public SpillableIndexOutputStream(FileIO fileIO, Path path, int threshold) {
        this.fileIO = fileIO;
        this.path = path;
        this.threshold = threshold;
    }

    @Override
    public void write(int value) throws IOException {
        if (fileOutput == null && buffer.size() >= threshold) {
            spill();
        }
        if (fileOutput == null) {
            buffer.write(value);
        } else {
            fileOutput.write(value);
        }
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        if (fileOutput == null && (long) buffer.size() + length > threshold) {
            spill();
        }
        if (fileOutput == null) {
            buffer.write(bytes, offset, length);
        } else {
            fileOutput.write(bytes, offset, length);
        }
    }

    private void spill() throws IOException {
        fileOutput = fileIO.newOutputStream(path, true);
        fileCreated = true;
        buffer.writeTo(fileOutput);
        buffer = new ByteArrayOutputStream();
    }

    public boolean spilled() {
        return fileCreated;
    }

    public byte[] embeddedBytes() {
        return buffer.toByteArray();
    }

    @Override
    public void flush() throws IOException {
        if (fileOutput != null) {
            fileOutput.flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (fileOutput != null) {
                fileOutput.close();
            }
        }
    }

    /** Removes an unpublished independent file after a failed write. */
    public void abort() throws IOException {
        try {
            close();
        } finally {
            if (fileCreated) {
                if (!fileIO.delete(path, false) && fileIO.exists(path)) {
                    throw new IOException("Failed to delete partial file index file " + path);
                }
            }
        }
    }
}
