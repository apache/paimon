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

package org.apache.paimon.format.shredding;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.utils.InternalRowUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Buffers initial rows, infers a per-file shredding write plan, and writes physical rows. */
public class InferShreddingWritePlanWriter implements BundleFormatWriter {

    private final SupportsShreddingWritePlan writerFactory;
    private final ShreddingWritePlanFactory writePlanFactory;
    private final PositionOutputStream out;
    private final String compression;

    private final List<InternalRow> bufferedRows;
    private final List<BundleRecords> bufferedBundles;

    @Nullable private FormatWriter actualWriter;
    private boolean planFinalized = false;
    private long totalBufferedRowCount = 0;

    public InferShreddingWritePlanWriter(
            SupportsShreddingWritePlan writerFactory,
            ShreddingWritePlanFactory writePlanFactory,
            PositionOutputStream out,
            String compression) {
        this.writerFactory = writerFactory;
        this.writePlanFactory = writePlanFactory;
        this.out = out;
        this.compression = compression;
        this.bufferedRows = new ArrayList<>();
        this.bufferedBundles = new ArrayList<>();
    }

    @Override
    public void addElement(InternalRow row) throws IOException {
        if (!planFinalized) {
            bufferedRows.add(
                    InternalRowUtils.copyInternalRow(row, writePlanFactory.logicalRowType()));
            totalBufferedRowCount++;
            if (totalBufferedRowCount >= writePlanFactory.inferBufferRowCount()) {
                finalizePlanAndFlush();
            }
            return;
        }

        actualWriter.addElement(row);
    }

    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        if (!planFinalized) {
            final List<InternalRow> rows = new ArrayList<>();
            for (InternalRow row : bundle) {
                rows.add(InternalRowUtils.copyInternalRow(row, writePlanFactory.logicalRowType()));
            }
            bufferedBundles.add(new CopiedBundleRecords(rows));
            totalBufferedRowCount += bundle.rowCount();
            if (totalBufferedRowCount >= writePlanFactory.inferBufferRowCount()) {
                finalizePlanAndFlush();
            }
            return;
        }

        ((BundleFormatWriter) actualWriter).writeBundle(bundle);
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        if (!planFinalized) {
            return false;
        }
        return actualWriter.reachTargetSize(suggestedCheck, targetSize);
    }

    @Nullable
    @Override
    public Object writerMetadata() {
        return actualWriter == null ? null : actualWriter.writerMetadata();
    }

    @Override
    public void close() throws IOException {
        try {
            if (!planFinalized) {
                finalizePlanAndFlush();
            }
        } finally {
            if (actualWriter != null) {
                actualWriter.close();
            }
        }
    }

    private void finalizePlanAndFlush() throws IOException {
        ShreddingWritePlan writePlan = writePlanFactory.createWritePlan(collectAllRows());
        actualWriter =
                ShreddingWritePlanWriterFactory.createWriterWithPlan(
                        writerFactory, out, compression, writePlan);
        planFinalized = true;

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
        if (bufferedBundles.isEmpty()) {
            return bufferedRows;
        }

        List<InternalRow> allRows = new ArrayList<>();
        for (BundleRecords bundle : bufferedBundles) {
            for (InternalRow row : bundle) {
                allRows.add(row);
            }
        }
        return allRows;
    }

    private static class CopiedBundleRecords implements BundleRecords {

        private final List<InternalRow> rows;

        private CopiedBundleRecords(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        @Nonnull
        public Iterator<InternalRow> iterator() {
            return rows.iterator();
        }

        @Override
        public long rowCount() {
            return rows.size();
        }
    }
}
