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

package org.apache.paimon.fileindex;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Binary compatibility tests for {@link FileIndexer}. */
public class FileIndexerCompatibilityTest {

    private static final String LEGACY_INDEX = "legacy-binary";

    @TempDir private Path temporaryDirectory;

    @Test
    public void testLegacyBinaryReadsV1Container() throws Exception {
        FileIndexer legacyIndexer = compileLegacyIndexer();
        FileIndexerFactory legacyFactory =
                new FileIndexerFactory() {
                    @Override
                    public String identifier() {
                        return LEGACY_INDEX;
                    }

                    @Override
                    public FileIndexer create(DataType type, Options options) {
                        return legacyIndexer;
                    }
                };

        Field factoriesField = FileIndexerFactoryUtils.class.getDeclaredField("factories");
        factoriesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, FileIndexerFactory> factories =
                (Map<String, FileIndexerFactory>) factoriesField.get(null);
        FileIndexerFactory previous = factories.put(LEGACY_INDEX, legacyFactory);
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, 1)) {
                writer.writeIndex("f0", LEGACY_INDEX, stream -> stream.write(42));
                writer.finish();
            }

            byte[] bytes = output.toByteArray();
            RowType rowType = RowType.builder().field("f0", DataTypes.INT()).build();
            try (FileIndexFormat.Reader reader =
                    FileIndexFormat.createReader(
                            new ByteArraySeekableStream(bytes), rowType, bytes.length)) {
                assertThat(reader.readColumnIndex("f0")).hasSize(1);
            }
        } finally {
            if (previous == null) {
                factories.remove(LEGACY_INDEX);
            } else {
                factories.put(LEGACY_INDEX, previous);
            }
        }
    }

    private FileIndexer compileLegacyIndexer() throws Exception {
        Path sourceDirectory = temporaryDirectory.resolve("source");
        Path classesDirectory = temporaryDirectory.resolve("classes");
        Path oldInterface = sourceDirectory.resolve("org/apache/paimon/fileindex/FileIndexer.java");
        Path legacyImplementation =
                sourceDirectory.resolve(
                        "org/apache/paimon/fileindex/compatible/LegacyFileIndexer.java");
        Files.createDirectories(oldInterface.getParent());
        Files.createDirectories(legacyImplementation.getParent());
        Files.createDirectories(classesDirectory);

        Files.write(
                oldInterface,
                Arrays.asList(
                        "package org.apache.paimon.fileindex;",
                        "import org.apache.paimon.fs.SeekableInputStream;",
                        "public interface FileIndexer {",
                        "  FileIndexWriter createWriter();",
                        "  FileIndexReader createReader(",
                        "      SeekableInputStream inputStream, int start, int length);",
                        "}"),
                StandardCharsets.UTF_8);
        Files.write(
                legacyImplementation,
                Arrays.asList(
                        "package org.apache.paimon.fileindex.compatible;",
                        "import org.apache.paimon.fileindex.FileIndexReader;",
                        "import org.apache.paimon.fileindex.FileIndexer;",
                        "import org.apache.paimon.fileindex.FileIndexWriter;",
                        "import org.apache.paimon.fs.SeekableInputStream;",
                        "public class LegacyFileIndexer implements FileIndexer {",
                        "  public FileIndexWriter createWriter() { return null; }",
                        "  public FileIndexReader createReader(",
                        "      SeekableInputStream input, int start, int length) {",
                        "    try {",
                        "      input.seek(start);",
                        "      if (length != 1 || input.read() != 42) {",
                        "        throw new RuntimeException(\"Unexpected legacy payload\");",
                        "      }",
                        "      return new FileIndexReader() {};",
                        "    } catch (java.io.IOException e) {",
                        "      throw new RuntimeException(e);",
                        "    }",
                        "  }",
                        "}"),
                StandardCharsets.UTF_8);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertThat(compiler).as("A JDK compiler is required").isNotNull();
        try (StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8)) {
            boolean compiled =
                    compiler.getTask(
                                    null,
                                    fileManager,
                                    null,
                                    Arrays.asList(
                                            "-classpath",
                                            System.getProperty("java.class.path"),
                                            "-d",
                                            classesDirectory.toString(),
                                            "-source",
                                            "8",
                                            "-target",
                                            "8",
                                            "-Xlint:-options"),
                                    null,
                                    fileManager.getJavaFileObjects(
                                            oldInterface.toFile(), legacyImplementation.toFile()))
                            .call();
            assertThat(compiled).isTrue();
        }

        URL[] urls = {classesDirectory.toUri().toURL()};
        try (URLClassLoader classLoader =
                new URLClassLoader(urls, FileIndexer.class.getClassLoader())) {
            Class<?> legacyClass =
                    classLoader.loadClass(
                            "org.apache.paimon.fileindex.compatible.LegacyFileIndexer");
            assertThat(
                            legacyClass.getDeclaredMethod(
                                    "createReader",
                                    org.apache.paimon.fs.SeekableInputStream.class,
                                    int.class,
                                    int.class))
                    .isNotNull();
            return (FileIndexer) legacyClass.getDeclaredConstructor().newInstance();
        }
    }
}
