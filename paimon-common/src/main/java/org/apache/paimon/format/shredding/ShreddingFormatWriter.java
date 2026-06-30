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
import org.apache.paimon.io.BundleRecords;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

/** A writer wrapper that converts logical rows to the physical row layout before writing. */
public class ShreddingFormatWriter implements BundleFormatWriter {

    private final FormatWriter delegate;
    private final SupportsShreddingWritePlan writerFactory;
    private final ShreddingWritePlan writePlan;
    private final String compression;

    public ShreddingFormatWriter(
            FormatWriter delegate,
            SupportsShreddingWritePlan writerFactory,
            ShreddingWritePlan writePlan,
            String compression) {
        this.delegate = delegate;
        this.writerFactory = writerFactory;
        this.writePlan = writePlan;
        this.compression = compression;
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        delegate.addElement(writePlan.toPhysicalRow(element));
    }

    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        if (delegate instanceof BundleFormatWriter) {
            ((BundleFormatWriter) delegate).writeBundle(new PhysicalBundleRecords(bundle));
            return;
        }

        for (InternalRow row : bundle) {
            addElement(row);
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return delegate.reachTargetSize(suggestedCheck, targetSize);
    }

    @Nullable
    @Override
    public Object writerMetadata() {
        return delegate.writerMetadata();
    }

    @Override
    public void close() throws IOException {
        try {
            writerFactory.commitShreddingMetadata(delegate, writePlan, compression);
        } finally {
            delegate.close();
        }
    }

    private class PhysicalBundleRecords implements BundleRecords {

        private final BundleRecords delegateBundle;

        private PhysicalBundleRecords(BundleRecords delegateBundle) {
            this.delegateBundle = delegateBundle;
        }

        @Override
        @Nonnull
        public Iterator<InternalRow> iterator() {
            final Iterator<InternalRow> iterator = delegateBundle.iterator();
            return new Iterator<InternalRow>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public InternalRow next() {
                    return writePlan.toPhysicalRow(iterator.next());
                }
            };
        }

        @Override
        public long rowCount() {
            return delegateBundle.rowCount();
        }
    }
}
