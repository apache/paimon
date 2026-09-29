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

package org.apache.paimon.io;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexWriterContext;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.FileIndexerFactory;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link FileIndexerFactory} for tests which records, for each data file, the context and the
 * values received by each of its writers.
 */
public class ContextRecordingFileIndexerFactory implements FileIndexerFactory {

    public static final String IDENTIFIER = "context-recording";

    /** Recorded writers, keyed by the data file path. */
    private static final Map<String, List<RecordedWriter>> RECORDED = new ConcurrentHashMap<>();

    public static Map<String, List<RecordedWriter>> recorded() {
        return RECORDED;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public FileIndexer create(DataType dataType, Options options) {
        return new FileIndexer() {

            @Override
            public FileIndexWriter createWriter() {
                throw new UnsupportedOperationException("A data file context is expected.");
            }

            @Override
            public FileIndexWriter createWriter(FileIndexWriterContext context) {
                RecordedWriter recorded = new RecordedWriter(context.schemaId());
                RECORDED.computeIfAbsent(
                                context.dataFilePath().toString(),
                                k -> new CopyOnWriteArrayList<>())
                        .add(recorded);
                return new FileIndexWriter() {

                    @Override
                    public void write(Object key) {
                        recorded.values.add(key);
                    }

                    @Override
                    public byte[] serializedBytes() {
                        return new byte[] {0};
                    }
                };
            }

            @Override
            public FileIndexReader createReader(
                    SeekableInputStream inputStream, int start, int length) {
                return new FileIndexReader() {};
            }
        };
    }

    /** The context and the values received by one writer. */
    public static class RecordedWriter {

        public final long schemaId;
        public final List<Object> values = new ArrayList<>();

        private RecordedWriter(long schemaId) {
            this.schemaId = schemaId;
        }
    }
}
