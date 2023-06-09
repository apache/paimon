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

package org.apache.paimon.format.orc.writer;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.orc.EstimateOrcAvgWidthVisitor;
import org.apache.paimon.format.orc.OrcSchemaVisitor;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.StripeInformation;
import org.apache.orc.Writer;
import org.apache.orc.impl.writer.TreeWriter;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A {@link FormatWriter} implementation that writes data in ORC format. */
public class OrcBulkWriter implements FormatWriter {

    private final Writer writer;
    private final Vectorizer<InternalRow> vectorizer;
    private final VectorizedRowBatch rowBatch;
    private final int avgRowByteSize;
    private final TreeWriter treeWriter;
    private final List<OrcProto.StripeInformation> stripes;

    public OrcBulkWriter(Vectorizer<InternalRow> vectorizer, Writer writer) {
        this.vectorizer = checkNotNull(vectorizer);
        this.writer = checkNotNull(writer);
        this.rowBatch = vectorizer.getSchema().createRowBatch();

        // Configure the vectorizer with the writer so that users can add
        // metadata on the fly through the Vectorizer#vectorize(...) method.
        this.vectorizer.setWriter(this.writer);
        this.avgRowByteSize =
                OrcSchemaVisitor.visitSchema(writer.getSchema(), new EstimateOrcAvgWidthVisitor())
                        .stream()
                        .reduce(Integer::sum)
                        .orElse(0);
        // TODO: Turn to access these hidden field directly after upgrade to ORC 1.7.4
        this.treeWriter = getHiddenFiledInORC("treeWriter");
        this.stripes = getHiddenFiledInORC("stripes");
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        vectorizer.vectorize(element, rowBatch);
        if (rowBatch.size == rowBatch.getMaxSize()) {
            writer.addRowBatch(rowBatch);
            rowBatch.reset();
        }
    }

    @Override
    public void flush() throws IOException {
        if (rowBatch.size != 0) {
            writer.addRowBatch(rowBatch);
            rowBatch.reset();
        }
    }

    @Override
    public void finish() throws IOException {
        flush();
        writer.close();
    }

    @Override
    public long length() throws IOException {
        long estimateMemory = treeWriter.estimateMemory();

        long dataLength = 0;
        List<StripeInformation> stripes =
                Collections.unmodifiableList(OrcUtils.convertProtoStripesToStripes(this.stripes));
        if (!stripes.isEmpty()) {
            StripeInformation stripeInformation = stripes.get(stripes.size() - 1);
            dataLength =
                    stripeInformation != null
                            ? stripeInformation.getOffset() + stripeInformation.getLength()
                            : 0;
        }

        // This value is estimated, not actual.
        return (long)
                Math.ceil(
                        dataLength
                                + (estimateMemory + (long) rowBatch.size * avgRowByteSize) * 0.2);
    }

    @SuppressWarnings("unchecked")
    private <T> T getHiddenFiledInORC(String fieldName) {
        try {
            Field treeWriterField = writer.getClass().getDeclaredField(fieldName);
            AccessController.doPrivileged(
                    (PrivilegedAction<Void>)
                            () -> {
                                treeWriterField.setAccessible(true);
                                return null;
                            });
            return (T) treeWriterField.get(writer);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Cannot get " + fieldName + " from " + writer.getClass().getName(), e);
        }
    }
}
