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

import static org.apache.paimon.arrow.ArrowUtils.serializeToIpc;

/** Vortex records writer. */
public class VortexRecordsWriter implements BundleFormatWriter {

    private static final Logger LOG = LoggerFactory.getLogger(VortexRecordsWriter.class);

    private final ArrowFormatWriter arrowFormatWriter;
    private final VortexWriter nativeWriter;
    private final String path;
    private long bytesWritten = 0;
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
            writeVsr(((ArrowBundleRecords) bundleRecords).getVectorSchemaRoot());
        } else {
            for (InternalRow row : bundleRecords) {
                addElement(row);
            }
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
        // Note: bytesWritten tracks Arrow IPC serialized bytes, not the actual Vortex file size
        // (which may differ due to Vortex's own compression/encoding).
        return suggestedCheck && (bytesWritten > targetSize);
    }

    @Override
    public void close() throws IOException {
        flush();
        LOG.info("Jni cost: {}ms for file: {}", jniCost, path);
        long t1 = System.currentTimeMillis();
        nativeWriter.close();
        arrowFormatWriter.close();
        long closeCost = (System.currentTimeMillis() - t1);
        LOG.info("Close cost: {}ms for file: {}", closeCost, path);
    }

    private void flush() throws IOException {
        arrowFormatWriter.flush();
        if (!arrowFormatWriter.empty()) {
            writeVsr(arrowFormatWriter.getVectorSchemaRoot());
        }
        arrowFormatWriter.reset();
    }

    private void writeVsr(VectorSchemaRoot vsr) throws IOException {
        byte[] arrowData = serializeToIpc(vsr);
        bytesWritten += arrowData.length;
        long t1 = System.currentTimeMillis();
        nativeWriter.writeBatch(arrowData);
        jniCost += (System.currentTimeMillis() - t1);
    }
}
