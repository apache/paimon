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

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.types.RowType;

import dev.vortex.api.DType;
import dev.vortex.api.VortexWriter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Vortex records writer.
 *
 * <p>Hands record batches to the native Vortex writer via the synchronous {@code
 * writeBatch(byte[])} path: each batch is serialized to an Arrow IPC byte array and copied into
 * native memory by the JNI call. Once {@code writeBatch} returns, the byte array (and the source
 * Arrow buffers) are no longer referenced by the native side, so the underlying {@link
 * ArrowFormatWriter} can be reset and reused immediately. This trades the FFI zero-copy throughput
 * for bounded memory and simpler lifetime management — without it, native asynchronously borrows
 * the Arrow buffers and they must be kept alive until file close, which grows unboundedly with
 * batch count.
 */
public class VortexRecordsWriter implements BundleFormatWriter {

    private static final Logger LOG = LoggerFactory.getLogger(VortexRecordsWriter.class);

    private final ArrowFormatWriter arrowFormatWriter;
    private final VortexWriter nativeWriter;
    private final String path;
    private long jniCost = 0;

    public VortexRecordsWriter(
            RowType rowType,
            ArrowFormatWriter arrowFormatWriter,
            Path path,
            Map<String, String> storageOptions)
            throws IOException {
        this.arrowFormatWriter = arrowFormatWriter;
        this.path = path.toUri().toString();

        DType dtype = VortexTypeUtils.toDType(rowType);
        this.nativeWriter = VortexWriter.create(this.path, dtype, storageOptions);
    }

    @Override
    public void addElement(InternalRow internalRow) throws IOException {
        if (!arrowFormatWriter.write(internalRow)) {
            flush();
            if (!arrowFormatWriter.write(internalRow)) {
                throw new RuntimeException("Exception happens while write to vortex file");
            }
        }
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) throws IOException {
        if (bundleRecords instanceof ArrowBundleRecords) {
            flush();
            writeVsr(((ArrowBundleRecords) bundleRecords).getVectorSchemaRoot());
        } else {
            for (InternalRow row : bundleRecords) {
                addElement(row);
            }
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
        // Vortex applies its own compression/encoding, so in-memory Arrow size is much larger
        // than the actual file size on disk. Always return false to avoid rolling into small files.
        return false;
    }

    @Override
    public void close() throws IOException {
        long t1 = System.currentTimeMillis();
        Throwable throwable = null;

        try {
            flush();
        } catch (Throwable t) {
            throwable = t;
        }

        try {
            nativeWriter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            arrowFormatWriter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        long closeCost = System.currentTimeMillis() - t1;
        LOG.info("Jni cost: {}ms, close cost: {}ms for file: {}", jniCost, closeCost, path);

        if (throwable != null) {
            rethrow(throwable);
        }
    }

    private void flush() throws IOException {
        try {
            arrowFormatWriter.flush();
            if (!arrowFormatWriter.empty()) {
                writeVsr(arrowFormatWriter.getVectorSchemaRoot());
            }
        } finally {
            arrowFormatWriter.reset();
        }
    }

    private void writeVsr(VectorSchemaRoot vsr) throws IOException {
        byte[] bytes = ArrowUtils.serializeToIpc(vsr);
        long t1 = System.currentTimeMillis();
        nativeWriter.writeBatch(bytes);
        jniCost += (System.currentTimeMillis() - t1);
    }

    private static Throwable addSuppressed(Throwable throwable, Throwable suppressed) {
        if (throwable == null) {
            return suppressed;
        }
        throwable.addSuppressed(suppressed);
        return throwable;
    }

    private static void rethrow(Throwable throwable) throws IOException {
        if (throwable instanceof IOException) {
            throw (IOException) throwable;
        }
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        throw new IOException(throwable);
    }
}
