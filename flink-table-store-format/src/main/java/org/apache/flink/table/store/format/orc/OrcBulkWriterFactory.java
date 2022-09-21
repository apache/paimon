/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.format.orc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.orc.vector.Vectorizer;
import org.apache.flink.orc.writer.PhysicalWriterImpl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;
import org.apache.orc.impl.WriterImpl;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Orc {@link BulkWriter.Factory}. The main code is copied from Flink {@code OrcBulkWriterFactory}.
 */
public class OrcBulkWriterFactory<T> implements BulkWriter.Factory<T> {

    private final Vectorizer<T> vectorizer;
    private final OrcFile.WriterOptions writerOptions;

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer, ORC WriterOptions.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     * @param writerOptions ORC WriterOptions.
     */
    public OrcBulkWriterFactory(Vectorizer<T> vectorizer, OrcFile.WriterOptions writerOptions) {
        this.vectorizer = checkNotNull(vectorizer);
        this.writerOptions = checkNotNull(writerOptions);
    }

    @Override
    public BulkWriter<T> create(FSDataOutputStream out) throws IOException {
        OrcFile.WriterOptions opts = getWriterOptions();
        opts.physicalWriter(new PhysicalWriterImpl(out, opts));

        // The path of the Writer is not used to indicate the destination file
        // in this case since we have used a dedicated physical writer to write
        // to the give output stream directly. However, the path would be used as
        // the key of writer in the ORC memory manager, thus we need to make it unique.
        Path unusedPath = new Path(UUID.randomUUID().toString());
        return new OrcBulkWriter<>(vectorizer, new WriterImpl(null, unusedPath, opts));
    }

    @VisibleForTesting
    protected OrcFile.WriterOptions getWriterOptions() {
        return writerOptions;
    }

    /** Orc {@link BulkWriter}. The main code is copied from Flink {@code OrcBulkWriter}. */
    private static class OrcBulkWriter<T> implements BulkWriter<T> {

        private final Writer writer;
        private final Vectorizer<T> vectorizer;
        private final VectorizedRowBatch rowBatch;

        OrcBulkWriter(Vectorizer<T> vectorizer, Writer writer) {
            this.vectorizer = checkNotNull(vectorizer);
            this.writer = checkNotNull(writer);
            this.rowBatch = vectorizer.getSchema().createRowBatch();

            // Configure the vectorizer with the writer so that users can add
            // metadata on the fly through the Vectorizer#vectorize(...) method.
            this.vectorizer.setWriter(this.writer);
        }

        @Override
        public void addElement(T element) throws IOException {
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
    }
}
