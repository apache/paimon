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

package org.apache.paimon.globalindex.testfulltext;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test full-text index writer that stores all text documents in memory and writes them to a single
 * binary file on {@link #finish()}.
 *
 * <p>Binary format (all values little-endian):
 *
 * <pre>
 *   [4 bytes] count (int)
 *   For each document:
 *     [4 bytes] text length in bytes (int)
 *     [N bytes] UTF-8 text
 * </pre>
 */
public class TestFullTextGlobalIndexWriter implements GlobalIndexSingletonWriter {

    private static final String FILE_NAME_PREFIX = "test-fulltext";

    private final GlobalIndexFileWriter fileWriter;
    private final List<String> documents;

    public TestFullTextGlobalIndexWriter(GlobalIndexFileWriter fileWriter) {
        this.fileWriter = fileWriter;
        this.documents = new ArrayList<>();
    }

    @Override
    public void write(Object fieldData) {
        if (fieldData == null) {
            throw new IllegalArgumentException("Text field data must not be null");
        }

        String text;
        if (fieldData instanceof String) {
            text = (String) fieldData;
        } else if (fieldData instanceof BinaryString) {
            text = fieldData.toString();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported text type: " + fieldData.getClass().getName());
        }
        documents.add(text);
    }

    @Override
    public List<ResultEntry> finish() {
        if (documents.isEmpty()) {
            return Collections.emptyList();
        }

        try {
            String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                // Header: count
                ByteBuffer header = ByteBuffer.allocate(4);
                header.order(ByteOrder.LITTLE_ENDIAN);
                header.putInt(documents.size());
                out.write(header.array());

                // Documents
                for (String doc : documents) {
                    byte[] textBytes = doc.getBytes(StandardCharsets.UTF_8);
                    ByteBuffer lenBuf = ByteBuffer.allocate(4);
                    lenBuf.order(ByteOrder.LITTLE_ENDIAN);
                    lenBuf.putInt(textBytes.length);
                    out.write(lenBuf.array());
                    out.write(textBytes);
                }
                out.flush();
            }

            return Collections.singletonList(new ResultEntry(fileName, documents.size(), null));
        } catch (IOException e) {
            throw new RuntimeException("Failed to write test full-text index", e);
        }
    }
}
