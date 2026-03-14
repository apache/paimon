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

package org.apache.paimon.arrow.writer;

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.vector.ArrowCStruct;
import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.BundleRecords;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Arrow bundle writer. */
public class ArrowBundleWriter implements BundleFormatWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowBundleWriter.class);

    private final PositionOutputStream underlyingStream;
    private final NativeWriter nativeWriter;

    private final ArrowFormatCWriter arrowFormatWriter;

    private long serializeCost = 0;
    private long jniCost = 0;

    public ArrowBundleWriter(
            PositionOutputStream positionOutputStream,
            ArrowFormatCWriter arrowFormatWriter,
            NativeWriter nativeWriter) {
        this.underlyingStream = positionOutputStream;
        this.arrowFormatWriter = arrowFormatWriter;
        this.nativeWriter = nativeWriter;
    }

    @Override
    public void addElement(InternalRow internalRow) {
        if (!arrowFormatWriter.write(internalRow)) {
            flush();
            if (!arrowFormatWriter.write(internalRow)) {
                throw new RuntimeException("Exception happens while write to orc file");
            }
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

    public void add(VectorSchemaRoot vsr) {
        long t1 = System.currentTimeMillis();
        BufferAllocator bufferAllocator = vsr.getVector(0).getAllocator();
        try (ArrowArray array = ArrowArray.allocateNew(bufferAllocator);
                ArrowSchema schema = ArrowSchema.allocateNew(bufferAllocator)) {
            ArrowCStruct struct =
                    ArrowUtils.serializeToCStruct(vsr, array, schema, bufferAllocator);
            long t2 = System.currentTimeMillis();
            serializeCost += (t2 - t1);
            this.nativeWriter.writeIpcBytes(struct.arrayAddress(), struct.schemaAddress());
            array.release();
            schema.release();
            jniCost += (System.currentTimeMillis() - t2);
        } catch (RuntimeException e) {
            LOG.error("Exception happens while add vsr", e);
            throw e;
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return suggestedCheck && (underlyingStream.getPos() > targetSize);
    }

    @Override
    public void close() throws IOException {
        flush();
        System.out.println("Serialize vsr cost: " + serializeCost + "ms");
        System.out.println("Jni cost: " + jniCost + "ms");
        this.nativeWriter.close();
        this.arrowFormatWriter.close();
    }

    private void flush() {
        try {
            if (!arrowFormatWriter.empty()) {
                long t1 = System.currentTimeMillis();
                arrowFormatWriter.flush();
                ArrowCStruct struct = arrowFormatWriter.toCStruct();
                long t2 = System.currentTimeMillis();
                serializeCost += (t2 - t1);
                this.nativeWriter.writeIpcBytes(struct.arrayAddress(), struct.schemaAddress());
                jniCost += (System.currentTimeMillis() - t2);
                arrowFormatWriter.reset();
            }
        } catch (RuntimeException e) {
            LOG.error("Exception happens while flush to file", e);
            throw e;
        }
    }

    public NativeWriter getNativeWriter() {
        return nativeWriter;
    }
}
