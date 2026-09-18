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

package org.apache.paimon.manifest;

import org.apache.paimon.format.avro.AvroBlockWriter;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.format.avro.AvroRawBlock;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.PathFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/** Avro writer for materialized, encoded and compressed index manifest entries. */
public final class IndexManifestAvroWriter implements AutoCloseable {

    private final FileIO fileIO;
    private final ObjectSerializer<IndexManifestEntry> serializer;
    private final Path path;

    private @Nullable PositionOutputStream out;
    private @Nullable AvroBlockWriter writer;
    private boolean closed;
    private boolean aborted;

    IndexManifestAvroWriter(
            FileIO fileIO,
            AvroFileFormat avroFileFormat,
            ObjectSerializer<IndexManifestEntry> serializer,
            String compression,
            PathFactory pathFactory) {
        this.fileIO = fileIO;
        this.serializer = serializer;
        this.path = pathFactory.newPath();

        boolean outputCreated = false;
        try {
            out = fileIO.newOutputStream(path, false);
            outputCreated = true;
            writer =
                    avroFileFormat.createBlockWriter(
                            out, IndexManifestEntry.MANIFEST_ROW_TYPE, compression);
        } catch (IOException failure) {
            UncheckedIOException primaryFailure =
                    new UncheckedIOException(
                            "Failed to create index manifest Avro writer for " + path, failure);
            abortCollecting(primaryFailure, outputCreated);
            throw primaryFailure;
        } catch (RuntimeException | Error failure) {
            abortCollecting(failure, outputCreated);
            throw failure;
        }
    }

    public void write(IndexManifestEntry entry) throws IOException {
        try {
            ensureOpen().addElement(serializer.toRow(entry));
        } catch (IOException | RuntimeException | Error failure) {
            abort(failure);
            throw failure;
        }
    }

    public void writeEncoded(ByteBuffer encodedRecord) throws IOException {
        try {
            ensureOpen().addEncoded(encodedRecord);
        } catch (IOException | RuntimeException | Error failure) {
            abort(failure);
            throw failure;
        }
    }

    public void writeEncodedBlock(AvroRawBlock block) throws IOException {
        try {
            ensureOpen().addEncodedBlock(block);
        } catch (IOException | RuntimeException | Error failure) {
            abort(failure);
            throw failure;
        }
    }

    public String result() {
        if (!closed || aborted) {
            throw new IllegalStateException(
                    "Cannot access index manifest result before closing the writer.");
        }
        return path.getName();
    }

    public void abort() {
        Throwable cleanupFailure = abortCollecting(null, true);
        if (cleanupFailure != null) {
            ExceptionUtils.rethrow(cleanupFailure);
        }
    }

    /** Aborts this writer and attaches cleanup failures to the primary failure. */
    public void abort(Throwable primaryFailure) {
        abortCollecting(primaryFailure, true);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            AvroBlockWriter currentWriter = ensureOpen();
            writer = null;
            currentWriter.close();
            out.flush();
            out.close();
            out = null;
        } catch (IOException | RuntimeException | Error failure) {
            abortCollecting(failure, true);
            throw failure;
        } finally {
            closed = true;
        }
    }

    private AvroBlockWriter ensureOpen() {
        if (closed || writer == null) {
            throw new IllegalStateException("Index manifest writer has already closed.");
        }
        return writer;
    }

    private Throwable abortCollecting(@Nullable Throwable primaryFailure, boolean deletePath) {
        if (aborted) {
            return primaryFailure;
        }
        aborted = true;
        closed = true;

        AvroBlockWriter currentWriter = writer;
        writer = null;
        primaryFailure = closeCollecting(currentWriter, primaryFailure);

        PositionOutputStream currentOut = out;
        out = null;
        primaryFailure = closeCollecting(currentOut, primaryFailure);

        if (deletePath) {
            try {
                fileIO.deleteQuietly(path);
            } catch (Throwable cleanupFailure) {
                primaryFailure = ExceptionUtils.firstOrSuppressed(cleanupFailure, primaryFailure);
            }
        }
        return primaryFailure;
    }

    private static Throwable closeCollecting(
            @Nullable AutoCloseable closeable, @Nullable Throwable primaryFailure) {
        if (closeable == null) {
            return primaryFailure;
        }
        try {
            closeable.close();
        } catch (Throwable cleanupFailure) {
            primaryFailure = ExceptionUtils.firstOrSuppressed(cleanupFailure, primaryFailure);
        }
        return primaryFailure;
    }
}
