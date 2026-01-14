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

package org.apache.paimon.format.variant;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.InferVariantShreddingSchema;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A generic writer that infers the shredding schema from buffered rows before writing.
 *
 * <p>This writer buffers rows up to a threshold, infers the optimal schema from them, then writes
 * all data using the inferred schema. It works with any format that implements {@link
 * SupportsVariantInference}.
 */
public class InferVariantShreddingWriter implements BundleFormatWriter {

    private final SupportsVariantInference writerFactory;
    private final InferVariantShreddingSchema shreddingSchemaInfer;
    private final int maxBufferRow;
    private final PositionOutputStream out;
    private final String compression;

    private final List<InternalRow> bufferedRows;
    private final List<BundleRecords> bufferedBundles;

    private FormatWriter actualWriter;
    private boolean schemaFinalized = false;
    private long totalBufferedRowCount = 0;

    public InferVariantShreddingWriter(
            SupportsVariantInference writerFactory,
            InferVariantShreddingSchema shreddingSchemaInfer,
            int maxBufferRow,
            PositionOutputStream out,
            String compression) {
        this.writerFactory = writerFactory;
        this.shreddingSchemaInfer = shreddingSchemaInfer;
        this.maxBufferRow = maxBufferRow;
        this.out = out;
        this.compression = compression;
        this.bufferedRows = new ArrayList<>();
        this.bufferedBundles = new ArrayList<>();
    }

    @Override
    public void addElement(InternalRow row) throws IOException {
        if (!schemaFinalized) {
            bufferedRows.add(row);
            totalBufferedRowCount++;
            if (totalBufferedRowCount >= maxBufferRow) {
                finalizeSchemaAndFlush();
            }
        } else {
            actualWriter.addElement(row);
        }
    }

    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        if (!schemaFinalized) {
            bufferedBundles.add(bundle);
            totalBufferedRowCount += bundle.rowCount();
            if (totalBufferedRowCount >= maxBufferRow) {
                finalizeSchemaAndFlush();
            }
        } else {
            ((BundleFormatWriter) actualWriter).writeBundle(bundle);
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        if (!schemaFinalized) {
            return false;
        }
        return actualWriter.reachTargetSize(suggestedCheck, targetSize);
    }

    @Override
    public void close() throws IOException {
        try {
            if (!schemaFinalized) {
                finalizeSchemaAndFlush();
            }
        } finally {
            if (actualWriter != null) {
                actualWriter.close();
            }
        }
    }

    private void finalizeSchemaAndFlush() throws IOException {
        RowType inferredShreddingSchema = shreddingSchemaInfer.inferSchema(collectAllRows());
        actualWriter =
                writerFactory.createWithShreddingSchema(out, compression, inferredShreddingSchema);
        schemaFinalized = true;

        if (!bufferedBundles.isEmpty()) {
            BundleFormatWriter bundleWriter = (BundleFormatWriter) actualWriter;
            for (BundleRecords bundle : bufferedBundles) {
                bundleWriter.writeBundle(bundle);
            }
            bufferedBundles.clear();
        } else {
            for (InternalRow row : bufferedRows) {
                actualWriter.addElement(row);
            }
            bufferedRows.clear();
        }
    }

    private List<InternalRow> collectAllRows() {
        if (!bufferedBundles.isEmpty()) {
            List<InternalRow> allRows = new ArrayList<>();
            for (BundleRecords bundle : bufferedBundles) {
                for (InternalRow row : bundle) {
                    allRows.add(row);
                }
            }
            return allRows;
        } else {
            return bufferedRows;
        }
    }
}
