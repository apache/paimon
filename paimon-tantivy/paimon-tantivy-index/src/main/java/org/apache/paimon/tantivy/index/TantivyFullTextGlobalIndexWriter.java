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

package org.apache.paimon.tantivy.index;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.fulltext.FullTextIndexOutput;
import org.apache.paimon.index.fulltext.FullTextIndexWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Full-text global index writer using Tantivy.
 *
 * <p>Text data is written through the standalone paimon-full-text native writer.
 */
public class TantivyFullTextGlobalIndexWriter implements GlobalIndexSingleColumnWriter, Closeable {

    private static final String FILE_NAME_PREFIX = "tantivy";

    private final GlobalIndexFileWriter fileWriter;
    private final TantivyFullTextIndexOptions indexOptions;
    private FullTextIndexWriter writer;
    private long rowCount;
    private boolean closed;

    public TantivyFullTextGlobalIndexWriter(GlobalIndexFileWriter fileWriter) {
        this(fileWriter, TantivyFullTextIndexOptions.defaults());
    }

    public TantivyFullTextGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter, TantivyFullTextIndexOptions indexOptions) {
        this.fileWriter = fileWriter;
        this.indexOptions = indexOptions;
        this.rowCount = 0;
        this.closed = false;
        this.writer = FullTextIndexWriter.create(indexOptions.toNativeOptions());
    }

    @Override
    public void write(Object fieldData, long relativeRowId) {
        if (closed) {
            throw new IllegalStateException("TantivyFullTextGlobalIndexWriter is already closed.");
        }

        if (fieldData == null) {
            rowCount++;
            return;
        }

        String text;
        if (fieldData instanceof BinaryString) {
            text = fieldData.toString();
        } else if (fieldData instanceof String) {
            text = (String) fieldData;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported field type: " + fieldData.getClass().getName());
        }

        writer.addDocument(relativeRowId, text);
        rowCount++;
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (rowCount == 0) {
                return Collections.emptyList();
            }

            return Collections.singletonList(writeIndex());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Tantivy full-text global index", e);
        } finally {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            closed = true;
        }
    }

    private ResultEntry writeIndex() throws IOException {
        String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
        try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
            writer.writeIndex(
                    new FullTextIndexOutput() {
                        @Override
                        public void write(byte[] buffer, int offset, int length)
                                throws IOException {
                            out.write(buffer, offset, length);
                        }

                        @Override
                        public void flush() throws IOException {
                            out.flush();
                        }
                    });
            out.flush();
        }
        return new ResultEntry(fileName, rowCount, indexOptions.serialize());
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }
    }
}
