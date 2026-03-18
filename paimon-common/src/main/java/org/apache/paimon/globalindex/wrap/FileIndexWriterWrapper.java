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

package org.apache.paimon.globalindex.wrap;

import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;

import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

/** A {@link GlobalIndexReader} wrapper for {@link FileIndexWriter}. */
public class FileIndexWriterWrapper implements GlobalIndexSingletonWriter {

    private final GlobalIndexFileWriter fileWriter;
    private final FileIndexWriter writer;
    private final String indexType;
    private long count = 0;

    public FileIndexWriterWrapper(
            GlobalIndexFileWriter fileWriter, FileIndexWriter writer, String indexType) {
        this.fileWriter = fileWriter;
        this.writer = writer;
        this.indexType = indexType;
    }

    @Override
    public void write(Object key) {
        count++;
        writer.write(key);
    }

    @Override
    public List<ResultEntry> finish() {
        if (count > 0) {
            String fileName = fileWriter.newFileName(indexType);
            try (OutputStream outputStream = fileWriter.newOutputStream(fileName)) {
                outputStream.write(writer.serializedBytes());
            } catch (Exception e) {
                throw new RuntimeException("Failed to write global index file: " + fileName, e);
            }
            return Collections.singletonList(new ResultEntry(fileName, count, null));
        } else {
            return Collections.emptyList();
        }
    }
}
