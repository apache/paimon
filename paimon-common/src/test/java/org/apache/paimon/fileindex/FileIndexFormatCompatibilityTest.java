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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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

import static org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory.BITMAP_INDEX;
import static org.assertj.core.api.Assertions.assertThat;

/** Binary compatibility tests for {@link FileIndexFormat}. */
public class FileIndexFormatCompatibilityTest {

    @TempDir private Path temporaryDirectory;

    @Test
    public void testLegacyApi() throws Exception {
        Class<?> legacyClient = compileLegacyClient();
        Map<String, Map<String, byte[]>> indexes = new LinkedHashMap<>();
        indexes.put("f0", Collections.singletonMap("legacy", new byte[] {42}));

        byte[] factoryContainer = write(legacyClient, "writeWithFactory", indexes);
        assertThat(read(legacyClient, "readWithFactory", factoryContainer)).isEqualTo(42);
        assertThat(readMetadata(legacyClient, factoryContainer)).isEqualTo(1);

        byte[] constructorContainer = write(legacyClient, "writeWithConstructor", indexes);
        assertThat(read(legacyClient, "readWithConstructor", constructorContainer)).isEqualTo(42);

        Method createPredicate =
                legacyClient.getDeclaredMethod(
                        "createPredicate", SeekableInputStream.class, RowType.class);
        RowType rowType = RowType.builder().field("f0", DataTypes.INT()).build();
        try (FileIndexPredicate ignored =
                (FileIndexPredicate)
                        createPredicate.invoke(
                                null, new ByteArraySeekableStream(v1BitmapContainer()), rowType)) {
            PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
            assertThat(((BitmapIndexResult) ignored.evaluate(predicateBuilder.equal(0, 42))).get())
                    .isEqualTo(RoaringBitmap32.bitmapOf(0));
            assertThat(ignored.evaluate(predicateBuilder.equal(0, 43)).remain()).isFalse();
        }

        Method coreThreshold = legacyClient.getDeclaredMethod("coreThreshold", CoreOptions.class);
        assertThat((long) coreThreshold.invoke(null, new CoreOptions(new Options())))
                .isEqualTo(500L);
        Method fileIndexThreshold =
                legacyClient.getDeclaredMethod("fileIndexThreshold", FileIndexOptions.class);
        assertThat((long) fileIndexThreshold.invoke(null, new FileIndexOptions())).isEqualTo(500L);
    }

    private byte[] write(
            Class<?> legacyClient, String methodName, Map<String, Map<String, byte[]>> indexes)
            throws Exception {
        Method write = legacyClient.getDeclaredMethod(methodName, OutputStream.class, Map.class);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        write.invoke(null, output, indexes);
        return output.toByteArray();
    }

    private int read(Class<?> legacyClient, String methodName, byte[] container) throws Exception {
        Method read =
                legacyClient.getDeclaredMethod(
                        methodName, SeekableInputStream.class, RowType.class);
        return (int)
                read.invoke(
                        null, new ByteArraySeekableStream(container), RowType.builder().build());
    }

    private int readMetadata(Class<?> legacyClient, byte[] container) throws Exception {
        Method readMetadata =
                legacyClient.getDeclaredMethod("readMetadata", SeekableInputStream.class);
        return (int) readMetadata.invoke(null, new ByteArraySeekableStream(container));
    }

    private byte[] v1BitmapContainer() throws IOException {
        FileIndexWriter indexWriter =
                new BitmapFileIndex(DataTypes.INT(), new Options()).createWriter();
        indexWriter.writeRecord(42);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, 1)) {
            writer.writeIndex("f0", BITMAP_INDEX, indexWriter::writeTo);
            writer.finish();
        }
        return output.toByteArray();
    }

    private Class<?> compileLegacyClient() throws Exception {
        Path sourceDirectory = temporaryDirectory.resolve("source");
        Path classesDirectory = temporaryDirectory.resolve("classes");
        Path oldFormat =
                sourceDirectory.resolve("org/apache/paimon/fileindex/FileIndexFormat.java");
        Path oldPredicate =
                sourceDirectory.resolve("org/apache/paimon/fileindex/FileIndexPredicate.java");
        Path oldCoreOptions = sourceDirectory.resolve("org/apache/paimon/CoreOptions.java");
        Path oldFileIndexOptions =
                sourceDirectory.resolve("org/apache/paimon/fileindex/FileIndexOptions.java");
        Path legacyClient =
                sourceDirectory.resolve(
                        "org/apache/paimon/fileindex/compatible/LegacyFileIndexFormatClient.java");
        Files.createDirectories(oldFormat.getParent());
        Files.createDirectories(legacyClient.getParent());
        Files.createDirectories(classesDirectory);

        Files.write(
                oldFormat,
                Arrays.asList(
                        "package org.apache.paimon.fileindex;",
                        "import java.io.Closeable;",
                        "import java.io.IOException;",
                        "import java.io.OutputStream;",
                        "import java.util.List;",
                        "import java.util.Map;",
                        "import org.apache.paimon.fs.SeekableInputStream;",
                        "import org.apache.paimon.types.RowType;",
                        "public final class FileIndexFormat {",
                        "  public static Writer createWriter(OutputStream output) { return null; }",
                        "  public static Reader createReader(SeekableInputStream input, RowType type) { return null; }",
                        "  public static Reader createMetadataReader(SeekableInputStream input) { return null; }",
                        "  public static class Writer implements Closeable {",
                        "    public Writer(OutputStream output) {}",
                        "    public void writeColumnIndexes(Map<String, Map<String, byte[]>> indexes)",
                        "        throws IOException {}",
                        "    public void close() throws IOException {}",
                        "  }",
                        "  public static class Reader implements Closeable {",
                        "    public Reader(SeekableInputStream input, RowType type) {}",
                        "    public Map<String, Map<String, byte[]>> readAll() { return null; }",
                        "    public List<FileIndexMeta> indexMetas() { return null; }",
                        "    public void close() throws IOException {}",
                        "  }",
                        "  public static class FileIndexMeta {",
                        "    public int sizeInBytes() { return 0; }",
                        "  }",
                        "}"),
                StandardCharsets.UTF_8);
        Files.write(
                oldPredicate,
                Arrays.asList(
                        "package org.apache.paimon.fileindex;",
                        "import java.io.Closeable;",
                        "import java.io.IOException;",
                        "import org.apache.paimon.fs.SeekableInputStream;",
                        "import org.apache.paimon.types.RowType;",
                        "public class FileIndexPredicate implements Closeable {",
                        "  public FileIndexPredicate(SeekableInputStream input, RowType type) {}",
                        "  public void close() throws IOException {}",
                        "}"),
                StandardCharsets.UTF_8);
        Files.write(
                oldCoreOptions,
                Arrays.asList(
                        "package org.apache.paimon;",
                        "public class CoreOptions {",
                        "  public long fileIndexInManifestThreshold() { return 0; }",
                        "}"),
                StandardCharsets.UTF_8);
        Files.write(
                oldFileIndexOptions,
                Arrays.asList(
                        "package org.apache.paimon.fileindex;",
                        "public class FileIndexOptions {",
                        "  public long fileIndexInManifestThreshold() { return 0; }",
                        "}"),
                StandardCharsets.UTF_8);
        Files.write(
                legacyClient,
                Arrays.asList(
                        "package org.apache.paimon.fileindex.compatible;",
                        "import java.io.IOException;",
                        "import java.io.OutputStream;",
                        "import java.util.Map;",
                        "import org.apache.paimon.CoreOptions;",
                        "import org.apache.paimon.fileindex.FileIndexFormat;",
                        "import org.apache.paimon.fileindex.FileIndexOptions;",
                        "import org.apache.paimon.fileindex.FileIndexPredicate;",
                        "import org.apache.paimon.fs.SeekableInputStream;",
                        "import org.apache.paimon.types.RowType;",
                        "public class LegacyFileIndexFormatClient {",
                        "  public static void writeWithFactory(",
                        "      OutputStream output, Map<String, Map<String, byte[]>> indexes)",
                        "      throws IOException {",
                        "    try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output)) {",
                        "      writer.writeColumnIndexes(indexes);",
                        "    }",
                        "  }",
                        "  public static void writeWithConstructor(",
                        "      OutputStream output, Map<String, Map<String, byte[]>> indexes)",
                        "      throws IOException {",
                        "    try (FileIndexFormat.Writer writer = new FileIndexFormat.Writer(output)) {",
                        "      writer.writeColumnIndexes(indexes);",
                        "    }",
                        "  }",
                        "  public static int readWithFactory(SeekableInputStream input, RowType type)",
                        "      throws IOException {",
                        "    try (FileIndexFormat.Reader reader = FileIndexFormat.createReader(input, type)) {",
                        "      return read(reader);",
                        "    }",
                        "  }",
                        "  public static int readWithConstructor(SeekableInputStream input, RowType type)",
                        "      throws IOException {",
                        "    try (FileIndexFormat.Reader reader = new FileIndexFormat.Reader(input, type)) {",
                        "      return read(reader);",
                        "    }",
                        "  }",
                        "  public static int readMetadata(SeekableInputStream input) throws IOException {",
                        "    try (FileIndexFormat.Reader reader = FileIndexFormat.createMetadataReader(input)) {",
                        "      return reader.indexMetas().get(0).sizeInBytes();",
                        "    }",
                        "  }",
                        "  private static int read(FileIndexFormat.Reader reader) {",
                        "    if (reader.indexMetas().get(0).sizeInBytes() != 1) {",
                        "      throw new AssertionError();",
                        "    }",
                        "    return reader.readAll().get(\"f0\").get(\"legacy\")[0];",
                        "  }",
                        "  public static FileIndexPredicate createPredicate(",
                        "      SeekableInputStream input, RowType type) {",
                        "    return new FileIndexPredicate(input, type);",
                        "  }",
                        "  public static long coreThreshold(CoreOptions options) {",
                        "    return options.fileIndexInManifestThreshold();",
                        "  }",
                        "  public static long fileIndexThreshold(FileIndexOptions options) {",
                        "    return options.fileIndexInManifestThreshold();",
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
                                            oldFormat.toFile(),
                                            oldPredicate.toFile(),
                                            oldCoreOptions.toFile(),
                                            oldFileIndexOptions.toFile(),
                                            legacyClient.toFile()))
                            .call();
            assertThat(compiled).isTrue();
        }

        URL[] urls = {classesDirectory.toUri().toURL()};
        try (URLClassLoader classLoader =
                new URLClassLoader(urls, FileIndexFormat.class.getClassLoader())) {
            return classLoader.loadClass(
                    "org.apache.paimon.fileindex.compatible.LegacyFileIndexFormatClient");
        }
    }
}
