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

    /** JNI 调用耗时（纳秒） */
    private long jniCostNanos = 0;
    /** Arrow 转换耗时（纳秒） */
    private long arrowConvertCostNanos = 0;
    /** flush 操作次数 */
    private long flushCount = 0;

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
        long t1 = System.nanoTime();
        boolean written = arrowFormatWriter.write(internalRow);
        arrowConvertCostNanos += (System.nanoTime() - t1);
        
        if (!written) {
            flush();
            t1 = System.nanoTime();
            if (!arrowFormatWriter.write(internalRow)) {
                throw new RuntimeException("Exception happens while write to lance file");
            }
            arrowConvertCostNanos += (System.nanoTime() - t1);
        }
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) throws IOException {
        if (bundleRecords instanceof ArrowBundleRecords) {
            add(((ArrowBundleRecords) bundleRecords).getVectorSchemaRoot());
        } else {
            for (InternalRow row : bundleRecords) {
                addElement(row);
            }
        }
    }

    public void add(VectorSchemaRoot vsr) throws IOException {
        long t1 = System.nanoTime();
        nativeWriter.writeVsr(vsr);
        jniCostNanos += (System.nanoTime() - t1);
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return suggestedCheck && (writtenPosition.getPosition() > targetSize);
    }

    @Override
    public void close() throws IOException {
        flush();
        logPerformanceMetrics();
        closeImpl();
    }

    /**
     * 输出性能统计日志。
     */
    private void logPerformanceMetrics() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Performance metrics for file: {}", nativeWriter.path());
            LOG.debug("  JNI cost: {} ms", jniCostNanos / 1_000_000);
            LOG.debug("  Arrow convert cost: {} ms", arrowConvertCostNanos / 1_000_000);
            LOG.debug("  Flush count: {}", flushCount);
        }
    }

    private void flush() throws IOException {
        long t1 = System.nanoTime();
        arrowFormatWriter.flush();
        arrowConvertCostNanos += (System.nanoTime() - t1);
        
        t1 = System.nanoTime();
        if (!arrowFormatWriter.empty()) {
            nativeWriter.writeVsr(arrowFormatWriter.getVectorSchemaRoot());
            flushCount++;
        }
        jniCostNanos += (System.nanoTime() - t1);
        arrowFormatWriter.reset();
    }

    private void closeImpl() throws IOException {
        long t1 = System.nanoTime();
        this.nativeWriter.close();
        this.arrowFormatWriter.close();
        long closeCostMs = (System.nanoTime() - t1) / 1_000_000;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Close cost: {} ms for file: {}", closeCostMs, nativeWriter.path());
        }
    }
}
