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
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Binary compatibility tests for {@link FileIndexFormat}. */
public class FileIndexFormatCompatibilityTest {

    @TempDir private Path temporaryDirectory;

    @Test
    public void testLegacyWriteColumnIndexes() throws Exception {
        Method legacyWrite = compileLegacyWriterClient();
        Map<String, Map<String, byte[]>> indexes = new LinkedHashMap<>();
        indexes.put("f0", Collections.singletonMap("legacy", new byte[] {42}));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, 1)) {
            legacyWrite.invoke(null, writer, indexes);
        }

        byte[] container = output.toByteArray();
        try (FileIndexFormat.Reader reader =
                FileIndexFormat.createReader(
                        new ByteArraySeekableStream(container),
                        RowType.builder().build(),
                        container.length)) {
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            reader.copyPayload("f0", "legacy", payload);
            assertThat(payload.toByteArray()).containsExactly(42);
        }
    }

    private Method compileLegacyWriterClient() throws Exception {
        Path sourceDirectory = temporaryDirectory.resolve("source");
        Path classesDirectory = temporaryDirectory.resolve("classes");
        Path oldFormat =
                sourceDirectory.resolve("org/apache/paimon/fileindex/FileIndexFormat.java");
        Path legacyClient =
                sourceDirectory.resolve(
                        "org/apache/paimon/fileindex/compatible/LegacyWriterClient.java");
        Files.createDirectories(oldFormat.getParent());
        Files.createDirectories(legacyClient.getParent());
        Files.createDirectories(classesDirectory);

        Files.write(
                oldFormat,
                Arrays.asList(
                        "package org.apache.paimon.fileindex;",
                        "import java.io.Closeable;",
                        "import java.io.IOException;",
                        "import java.util.Map;",
                        "public final class FileIndexFormat {",
                        "  public static class Writer implements Closeable {",
                        "    public void writeColumnIndexes(Map<String, Map<String, byte[]>> indexes)",
                        "        throws IOException {}",
                        "    public void close() throws IOException {}",
                        "  }",
                        "}"),
                StandardCharsets.UTF_8);
        Files.write(
                legacyClient,
                Arrays.asList(
                        "package org.apache.paimon.fileindex.compatible;",
                        "import java.io.IOException;",
                        "import java.util.Map;",
                        "import org.apache.paimon.fileindex.FileIndexFormat;",
                        "public class LegacyWriterClient {",
                        "  public static void write(",
                        "      FileIndexFormat.Writer writer,",
                        "      Map<String, Map<String, byte[]>> indexes) throws IOException {",
                        "    writer.writeColumnIndexes(indexes);",
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
                                            "-d",
                                            classesDirectory.toString(),
                                            "-source",
                                            "8",
                                            "-target",
                                            "8",
                                            "-Xlint:-options"),
                                    null,
                                    fileManager.getJavaFileObjects(
                                            oldFormat.toFile(), legacyClient.toFile()))
                            .call();
            assertThat(compiled).isTrue();
        }

        URL[] urls = {classesDirectory.toUri().toURL()};
        try (URLClassLoader classLoader =
                new URLClassLoader(urls, FileIndexFormat.class.getClassLoader())) {
            Class<?> client =
                    classLoader.loadClass(
                            "org.apache.paimon.fileindex.compatible.LegacyWriterClient");
            return client.getDeclaredMethod("write", FileIndexFormat.Writer.class, Map.class);
        }
    }
}
