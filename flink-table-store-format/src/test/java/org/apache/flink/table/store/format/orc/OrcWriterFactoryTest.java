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

package org.apache.flink.table.store.format.orc;

import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.table.store.format.orc.writer.Vectorizer;

import org.apache.hadoop.fs.Path;
import org.apache.orc.MemoryManager;
import org.apache.orc.OrcFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the behavior of {@link OrcWriterFactory}. */
class OrcWriterFactoryTest {

    @Test
    void testNotOverrideInMemoryManager(@TempDir java.nio.file.Path tmpDir) throws IOException {
        TestMemoryManager memoryManager = new TestMemoryManager();
        OrcWriterFactory<Record> factory =
                new TestOrcWriterFactory<>(
                        new RecordVectorizer("struct<_col0:string,_col1:int>"), memoryManager);
        factory.create(new LocalDataOutputStream(tmpDir.resolve("file1").toFile()));
        factory.create(new LocalDataOutputStream(tmpDir.resolve("file2").toFile()));

        List<Path> addedWriterPath = memoryManager.getAddedWriterPath();
        assertThat(addedWriterPath).hasSize(2);
        assertThat(addedWriterPath.get(1)).isNotEqualTo(addedWriterPath.get(0));
    }

    private static class TestOrcWriterFactory<T> extends OrcWriterFactory<T> {

        private final MemoryManager memoryManager;

        public TestOrcWriterFactory(Vectorizer<T> vectorizer, MemoryManager memoryManager) {
            super(vectorizer);
            this.memoryManager = checkNotNull(memoryManager);
        }

        @Override
        protected OrcFile.WriterOptions getWriterOptions() {
            OrcFile.WriterOptions options = super.getWriterOptions();
            options.memory(memoryManager);
            return options;
        }
    }

    private static class TestMemoryManager implements MemoryManager {
        private final List<Path> addedWriterPath = new ArrayList<>();

        @Override
        public void addWriter(Path path, long requestedAllocation, Callback callback) {
            addedWriterPath.add(path);
        }

        public List<Path> getAddedWriterPath() {
            return addedWriterPath;
        }

        @Override
        public void removeWriter(Path path) {}

        @Override
        public void addedRow(int rows) {}
    }
}
