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

package org.apache.paimon.format.lance;

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.lance.jni.LanceWriter;
import org.apache.paimon.io.BundleRecords;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Lance records writer. */
public class LanceRecordsWriter implements BundleFormatWriter {

    private static final Logger LOG = LoggerFactory.getLogger(LanceRecordsWriter.class);

    protected final WrittenPosition writtenPosition;
    protected final LanceWriter nativeWriter;

    private final ArrowFormatWriter arrowFormatWriter;

    private long jniCost = 0;

    public LanceRecordsWriter(
            WrittenPosition writtenPosition,
            ArrowFormatWriter arrowFormatWriter,
            LanceWriter nativeWriter) {
        this.writtenPosition = writtenPosition;
        this.arrowFormatWriter = arrowFormatWriter;
        this.nativeWriter = nativeWriter;
    }

    @Override
    public void addElement(InternalRow internalRow) throws IOException {
        if (!arrowFormatWriter.write(internalRow)) {
            flush();
            if (!arrowFormatWriter.write(internalRow)) {
                throw new RuntimeException("Exception happens while write to lance file");
            }
        }
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) throws IOException {
        if (bundleRecords instanceof ArrowBundleRecords) {
            ArrowBundleRecords arrowBundle = (ArrowBundleRecords) bundleRecords;
            VectorSchemaRoot root = arrowBundle.getVectorSchemaRoot();
            if (arrowFormatWriter.isArrowBundleSchemaCompatible(arrowBundle)
                    && ArrowUtils.hasSameRootAllocator(root, arrowFormatWriter.getAllocator())) {
                flush();
                nativeWriter.ensureInitialized(arrowFormatWriter.getAllocator());
                add(root);
                return;
            }
        }

        for (InternalRow row : bundleRecords) {
            addElement(row);
        }
    }

    public void add(VectorSchemaRoot vsr) throws IOException {
        long t1 = System.currentTimeMillis();
        nativeWriter.writeVsr(vsr);
        jniCost += (System.currentTimeMillis() - t1);
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return suggestedCheck && (writtenPosition.getPosition() > targetSize);
    }

    @Override
    public void close() throws IOException {
        Throwable throwable = null;

        try {
            flush();
        } catch (Throwable t) {
            throwable = t;
        }

        LOG.info("Jni cost: " + jniCost + "ms for file: " + nativeWriter.path());
        long t1 = System.currentTimeMillis();

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

        long closeCost = (System.currentTimeMillis() - t1);
        LOG.info("Close cost: " + closeCost + "ms for file: " + nativeWriter.path());

        if (throwable != null) {
            rethrow(throwable);
        }
    }

    private void flush() throws IOException {
        arrowFormatWriter.flush();
        long t1 = System.currentTimeMillis();
        if (!arrowFormatWriter.empty()) {
            nativeWriter.writeVsr(arrowFormatWriter.getVectorSchemaRoot());
        }
        jniCost += (System.currentTimeMillis() - t1);
        arrowFormatWriter.reset();
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
