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

import org.apache.commons.io.output.DeferredFileOutputStream;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** A bounded-memory BLOB staging area which spills larger payloads to a local temporary file. */
final class SpillableBlobStaging implements BlobStaging {

    private static final String TEMP_FILE_PREFIX = "paimon-blob-staging-";
    private static final String TEMP_FILE_SUFFIX = ".tmp";

    private final DeferredFileOutputStream output;

    private long length;
    private boolean finished;
    private boolean closed;

    SpillableBlobStaging(int memoryThreshold, @Nullable File tempDirectory) {
        checkArgument(
                memoryThreshold >= 0,
                "BLOB staging memory threshold must not be negative, but was %s.",
                memoryThreshold);
        this.output =
                new DeferredFileOutputStream(
                        memoryThreshold, TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX, tempDirectory);
    }

    @Override
    public void write(byte[] bytes, int offset, int bytesLength) throws IOException {
        checkState(!finished, "Cannot write to a finished BLOB staging area.");
        checkState(!closed, "Cannot write to a closed BLOB staging area.");
        output.write(bytes, offset, bytesLength);
        length += bytesLength;
    }

    @Override
    public void finish() throws IOException {
        checkState(!closed, "Cannot finish a closed BLOB staging area.");
        if (!finished) {
            output.close();
            finished = true;
        }
    }

    @Override
    public InputStream openInputStream() throws IOException {
        checkState(finished, "BLOB staging area must be finished before it is read.");
        checkState(!closed, "Cannot read a closed BLOB staging area.");
        if (output.isInMemory()) {
            return new ByteArrayInputStream(output.getData());
        }
        return new FileInputStream(output.getFile());
    }

    @Override
    public long length() {
        return length;
    }

    boolean isInMemory() {
        return output.isInMemory();
    }

    @Nullable
    File spillFile() {
        return output.getFile();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        IOException failure = null;
        if (!finished) {
            try {
                output.close();
            } catch (IOException e) {
                failure = e;
            }
        }

        File spillFile = output.getFile();
        if (spillFile != null) {
            try {
                Files.deleteIfExists(spillFile.toPath());
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }

        if (failure != null) {
            throw failure;
        }
    }
}
