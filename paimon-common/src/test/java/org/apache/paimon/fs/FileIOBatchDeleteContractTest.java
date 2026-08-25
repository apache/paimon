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

package org.apache.paimon.fs;

import org.apache.paimon.catalog.CatalogContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.IOException;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Public contract and binary compatibility tests for strict batch delete. */
class FileIOBatchDeleteContractTest {

    private static final Path FIRST = new Path("oss://bucket/table/a.parquet");
    private static final Path SECOND = new Path("oss://bucket/table/b.parquet");

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testLegacyImplementationUsesDefaultUnsupportedWithoutStorageAccess() throws Exception {
        LegacyFileIO legacy = new LegacyFileIO();

        Optional<BatchFileDeleter> capability = legacy.batchFileDeleter(FIRST);

        assertThat(capability).isEmpty();
        assertThat(legacy.storageCalls).hasValue(0);
    }

    @Test
    void testBatchDeleteResultDefensivelyCopiesAndDoesNotExposeMutableState() {
        List<Path> callerOwned = new ArrayList<>(Arrays.asList(FIRST, SECOND));

        BatchDeleteResult result = new BatchDeleteResult(callerOwned);
        callerOwned.clear();

        assertThat(result.deletedOrNotFound()).containsExactly(FIRST, SECOND);
        assertThatThrownBy(() -> result.deletedOrNotFound().add(FIRST))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> result.deletedOrNotFound().set(0, SECOND))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(result.deletedOrNotFound()).containsExactly(FIRST, SECOND);
    }

    @Test
    void testProviderCompiledAgainstOldInterfaceLoadsAndUsesNewDefaultMethod() throws Exception {
        java.nio.file.Path sources = Files.createDirectories(tempDir.resolve("sources"));
        java.nio.file.Path oldApiClasses = Files.createDirectories(tempDir.resolve("old-api"));
        java.nio.file.Path providerClasses = Files.createDirectories(tempDir.resolve("provider"));
        java.nio.file.Path oldInterface =
                writeSource(
                        sources,
                        "org/apache/paimon/fs/FileIO.java",
                        "package org.apache.paimon.fs;\n"
                                + "public interface FileIO extends java.io.Serializable {}\n");
        java.nio.file.Path oldProvider =
                writeSource(
                        sources,
                        "fixture/LegacyProvider.java",
                        "package fixture;\n"
                                + "public final class LegacyProvider "
                                + "implements org.apache.paimon.fs.FileIO {\n"
                                + "  public LegacyProvider() {}\n"
                                + "}\n");
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertThat(compiler).as("Maven tests must run on a JDK").isNotNull();
        assertThat(
                        compiler.run(
                                null,
                                null,
                                null,
                                "-d",
                                oldApiClasses.toString(),
                                oldInterface.toString()))
                .isZero();
        assertThat(
                        compiler.run(
                                null,
                                null,
                                null,
                                "-classpath",
                                oldApiClasses.toString(),
                                "-d",
                                providerClasses.toString(),
                                oldProvider.toString()))
                .isZero();

        // Parent-first loading replaces the compile-time interface with the current FileIO while
        // retaining provider bytecode compiled without the new method.
        try (URLClassLoader loader =
                new URLClassLoader(
                        new java.net.URL[] {providerClasses.toUri().toURL()},
                        FileIO.class.getClassLoader())) {
            Class<?> providerClass = Class.forName("fixture.LegacyProvider", true, loader);
            assertThat(providerClass.getInterfaces()).containsExactly(FileIO.class);
            FileIO provider = (FileIO) providerClass.getDeclaredConstructor().newInstance();

            assertThat(FileIO.class.getMethod("batchFileDeleter", Path.class).isDefault()).isTrue();
            assertThat(provider.batchFileDeleter(FIRST)).isEmpty();
        }
    }

    private static java.nio.file.Path writeSource(
            java.nio.file.Path root, String relative, String source) throws IOException {
        java.nio.file.Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.write(file, source.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    /**
     * This fixture intentionally does not override batchFileDeleter. Every observable storage
     * method fails, so even a harmless-looking capability probe has causal evidence.
     */
    private static class LegacyFileIO implements FileIO {

        private final AtomicInteger storageCalls = new AtomicInteger();

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        public void configure(CatalogContext context) {}

        @Override
        public SeekableInputStream newInputStream(Path path) {
            return unexpectedStorageCall("newInputStream");
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite) {
            return unexpectedStorageCall("newOutputStream");
        }

        @Override
        public FileStatus getFileStatus(Path path) {
            return unexpectedStorageCall("getFileStatus");
        }

        @Override
        public FileStatus[] listStatus(Path path) {
            return unexpectedStorageCall("listStatus");
        }

        @Override
        public boolean exists(Path path) {
            return unexpectedStorageCall("exists");
        }

        @Override
        public boolean delete(Path path, boolean recursive) {
            return unexpectedStorageCall("delete");
        }

        @Override
        public boolean mkdirs(Path path) {
            return unexpectedStorageCall("mkdirs");
        }

        @Override
        public boolean rename(Path src, Path dst) {
            return unexpectedStorageCall("rename");
        }

        private <T> T unexpectedStorageCall(String operation) {
            storageCalls.incrementAndGet();
            throw new AssertionError("Default capability accessed storage through " + operation);
        }
    }
}
