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

package org.apache.paimon.format.vortex;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * A factory to create Vortex {@link FormatWriter}.
 *
 * <p>A direct write lets local and OSS files use a native storage client. A write to a stream goes
 * through the stream, which its caller closes.
 */
public class VortexWriterFactory implements FormatWriterFactory, SupportsDirectWrite {

    private final Supplier<ArrowFormatCWriter> cWriterSupplier;
    private final int workerThreads;

    public VortexWriterFactory(Supplier<ArrowFormatCWriter> cWriterSupplier, int workerThreads) {
        this.cWriterSupplier = cWriterSupplier;
        this.workerThreads = workerThreads;
    }

    @VisibleForTesting
    Supplier<ArrowFormatCWriter> cWriterSupplier() {
        return cWriterSupplier;
    }

    @Override
    public FormatWriter create(PositionOutputStream positionOutputStream, String compression)
            throws IOException {
        VortexSessions.ensureWorkerThreads(workerThreads);
        return new VortexRecordsWriter(
                cWriterSupplier.get(), VortexOutput.of(positionOutputStream));
    }

    @Override
    public FormatWriter create(FileIO fileIO, Path path, String compression) throws IOException {
        VortexSessions.ensureWorkerThreads(workerThreads);
        return new VortexRecordsWriter(cWriterSupplier.get(), VortexOutput.of(fileIO, path));
    }
}
