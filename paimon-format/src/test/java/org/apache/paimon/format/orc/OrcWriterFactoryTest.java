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

package org.apache.paimon.format.orc;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.orc.writer.RowDataVectorizer;
import org.apache.paimon.format.orc.writer.Vectorizer;
import org.apache.paimon.fs.local.LocalFileIO.LocalPositionOutputStream;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.MemoryManager;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the behavior of {@link OrcWriterFactory}. */
class OrcWriterFactoryTest {

    @Test
    void testNotOverrideInMemoryManager(@TempDir java.nio.file.Path tmpDir) throws IOException {
        TestMemoryManager memoryManager = new TestMemoryManager();
        OrcWriterFactory factory =
                new TestOrcWriterFactory(
                        new RowDataVectorizer(
                                TypeDescription.fromString("struct<_col0:string,_col1:int>"),
                                Arrays.asList(
                                        new DataField(0, "f0", DataTypes.STRING()),
                                        new DataField(1, "f1", DataTypes.INT())),
                                true),
                        memoryManager);
        factory.create(new LocalPositionOutputStream(tmpDir.resolve("file1").toFile()), "LZ4");
        factory.create(new LocalPositionOutputStream(tmpDir.resolve("file2").toFile()), "LZ4");

        List<Path> addedWriterPath = memoryManager.getAddedWriterPath();
        assertThat(addedWriterPath).hasSize(2);
        assertThat(addedWriterPath.get(1)).isNotEqualTo(addedWriterPath.get(0));
    }

    private static class TestOrcWriterFactory extends OrcWriterFactory {

        private final MemoryManager memoryManager;

        public TestOrcWriterFactory(
                Vectorizer<InternalRow> vectorizer, MemoryManager memoryManager) {
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

    @Test
    void testWriterOptionsNotSharedBetweenCalls() {
        // create() writes per-file state into the options, so each caller needs its own.
        OrcWriterFactory factory =
                new OrcWriterFactory(
                        new RowDataVectorizer(
                                TypeDescription.createString(),
                                Collections.singletonList(
                                        new DataField(0, "f0", DataTypes.STRING())),
                                false));
        assertThat(factory.getWriterOptions()).isNotSameAs(factory.getWriterOptions());
    }

    @Test
    void testLowerCaseFileCompressionUnderTurkishLocale(@TempDir java.nio.file.Path tmpDir)
            throws IOException {
        // 'i' uppercases to 'İ' in Turkish, so a locale sensitive conversion turns zlib into
        // ZLİB and CompressionKind.valueOf rejects it
        CapturingOrcWriterFactory factory = capturingFactory(new Properties());
        withTurkishLocale(
                () -> factory.create(outputStream(tmpDir, "file-compression.orc"), "zlib").close());
        assertThat(factory.captured.getCompress()).isEqualTo(CompressionKind.ZLIB);
    }

    @Test
    void testLowerCaseOrcCompressPropertyUnderTurkishLocale(@TempDir java.nio.file.Path tmpDir)
            throws IOException {
        // the orc.compress table option is resolved by OrcFile.WriterOptions instead, one call
        // earlier than the branch above
        Properties properties = new Properties();
        properties.setProperty("orc.compress", "zlib");
        CapturingOrcWriterFactory factory = capturingFactory(properties);
        withTurkishLocale(
                () -> factory.create(outputStream(tmpDir, "orc-compress.orc"), "zstd").close());
        assertThat(factory.captured.getCompress()).isEqualTo(CompressionKind.ZLIB);
    }

    private static LocalPositionOutputStream outputStream(java.nio.file.Path tmpDir, String name)
            throws IOException {
        return new LocalPositionOutputStream(tmpDir.resolve(name).toFile());
    }

    private static CapturingOrcWriterFactory capturingFactory(Properties writerProperties) {
        return new CapturingOrcWriterFactory(
                new RowDataVectorizer(
                        TypeDescription.createString(),
                        Collections.singletonList(new DataField(0, "f0", DataTypes.STRING())),
                        false),
                writerProperties);
    }

    private static void withTurkishLocale(ThrowingRunnable body) throws IOException {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            body.run();
        } finally {
            Locale.setDefault(original);
        }
    }

    private interface ThrowingRunnable {
        void run() throws IOException;
    }

    private static class CapturingOrcWriterFactory extends OrcWriterFactory {

        private OrcFile.WriterOptions captured;

        private CapturingOrcWriterFactory(
                Vectorizer<InternalRow> vectorizer, Properties writerProperties) {
            super(
                    vectorizer,
                    writerProperties,
                    new Configuration(false),
                    1024,
                    MemorySize.ZERO,
                    false);
        }

        @Override
        protected OrcFile.WriterOptions getWriterOptions() {
            captured = super.getWriterOptions();
            return captured;
        }
    }
}
