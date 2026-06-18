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

package org.apache.paimon.format;

import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileFormatProvider}. */
public class FileFormatProviderTest {

    private static final String TEST_PROVIDER = "test-provider";
    private static final String DUPLICATE_PROVIDER = "duplicate-provider";
    private static final String PROVIDER_ONLY_FORMAT = "provider-only";
    private static final String DEFAULT_FORMAT = "default-format";

    @Test
    public void testProviderIsExplicitlySelectedWithoutHadoopClasses() throws Exception {
        try (URLClassLoader classLoader =
                new NoHadoopClassLoader(
                        classpathUrls(), FileFormatProviderTest.class.getClassLoader())) {
            Class<?> fileFormatClass = classLoader.loadClass(FileFormat.class.getName());
            Class<?> optionsClass = classLoader.loadClass(Options.class.getName());
            Method fromIdentifier =
                    fileFormatClass.getMethod("fromIdentifier", String.class, optionsClass);

            Object options = optionsClass.newInstance();
            optionsClass.getMethod("set", String.class, String.class)
                    .invoke(options, FileFormatProvider.FORMAT_PROVIDER, TEST_PROVIDER);
            Object fileFormat = fromIdentifier.invoke(null, PROVIDER_ONLY_FORMAT, options);

            assertThat(fileFormat.getClass().getName())
                    .isEqualTo(ProviderFileFormat.class.getName());
        }
    }

    @Test
    public void testProviderIsNotUsedWithoutExplicitSelection() throws Exception {
        try (URLClassLoader classLoader =
                new NoHadoopClassLoader(
                        classpathUrls(), FileFormatProviderTest.class.getClassLoader())) {
            Class<?> fileFormatClass = classLoader.loadClass(FileFormat.class.getName());
            Class<?> optionsClass = classLoader.loadClass(Options.class.getName());
            Method fromIdentifier =
                    fileFormatClass.getMethod("fromIdentifier", String.class, optionsClass);

            Object options = optionsClass.newInstance();

            try {
                fromIdentifier.invoke(null, PROVIDER_ONLY_FORMAT, options);
            } catch (ReflectiveOperationException e) {
                assertThat(e.getCause()).isNotNull();
                assertThat(e.getCause()).hasMessageContaining("Could not find any factory");
                return;
            }
        }

        throw new AssertionError("Provider should not be used without explicit selection.");
    }

    @Test
    public void testProviderFallsBackToDefaultFactoryWhenFormatIsNotHandled() {
        Options options = new Options();
        options.setString(FileFormatProvider.FORMAT_PROVIDER, TEST_PROVIDER);

        FileFormat fileFormat = FileFormat.fromIdentifier(DEFAULT_FORMAT, options);

        assertThat(fileFormat).isInstanceOf(FactoryFileFormat.class);
    }

    @Test
    public void testDuplicateProviderIdentifiersFailClearly() {
        Options options = new Options();
        options.setString(FileFormatProvider.FORMAT_PROVIDER, DUPLICATE_PROVIDER);

        assertThatThrownBy(() -> FileFormat.fromIdentifier(PROVIDER_ONLY_FORMAT, options))
                .hasMessageContaining("Multiple providers for identifier")
                .hasMessageContaining(DuplicateFileFormatProvider.class.getName())
                .hasMessageContaining(OtherDuplicateFileFormatProvider.class.getName());
    }

    private static URL[] classpathUrls() throws Exception {
        String[] entries = System.getProperty("java.class.path").split(File.pathSeparator);
        URL[] urls = new URL[entries.length];
        for (int i = 0; i < entries.length; i++) {
            urls[i] = new File(entries[i]).toURI().toURL();
        }
        return urls;
    }

    private static class NoHadoopClassLoader extends URLClassLoader {

        private static final List<String> PARENT_FIRST_PREFIXES =
                Collections.singletonList("org.junit.");

        private NoHadoopClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("org.apache.hadoop.")) {
                throw new ClassNotFoundException(name);
            }

            for (String prefix : PARENT_FIRST_PREFIXES) {
                if (name.startsWith(prefix)) {
                    return super.loadClass(name, resolve);
                }
            }

            synchronized (getClassLoadingLock(name)) {
                Class<?> loadedClass = findLoadedClass(name);
                if (loadedClass == null) {
                    try {
                        loadedClass = findClass(name);
                    } catch (ClassNotFoundException ignored) {
                        loadedClass = super.loadClass(name, false);
                    }
                }
                if (resolve) {
                    resolveClass(loadedClass);
                }
                return loadedClass;
            }
        }
    }

    public static class TestFileFormatProvider implements FileFormatProvider {

        @Override
        public String identifier() {
            return TEST_PROVIDER;
        }

        @Override
        public Optional<FileFormat> create(String identifier, FormatContext context) {
            if (PROVIDER_ONLY_FORMAT.equals(identifier)) {
                return Optional.of(new ProviderFileFormat(identifier));
            }
            return Optional.empty();
        }
    }

    public static class DuplicateFileFormatProvider implements FileFormatProvider {

        @Override
        public String identifier() {
            return DUPLICATE_PROVIDER;
        }

        @Override
        public Optional<FileFormat> create(String identifier, FormatContext context) {
            return Optional.of(new TestFileFormat(identifier));
        }
    }

    public static class OtherDuplicateFileFormatProvider extends DuplicateFileFormatProvider {}

    public static class TestFileFormatFactory implements FileFormatFactory {

        @Override
        public String identifier() {
            return DEFAULT_FORMAT;
        }

        @Override
        public FileFormat create(FormatContext formatContext) {
            return new FactoryFileFormat(DEFAULT_FORMAT);
        }
    }

    private static class ProviderFileFormat extends TestFileFormat {

        private ProviderFileFormat(String formatIdentifier) {
            super(formatIdentifier);
        }
    }

    private static class FactoryFileFormat extends TestFileFormat {

        private FactoryFileFormat(String formatIdentifier) {
            super(formatIdentifier);
        }
    }

    public static class TestFileFormat extends FileFormat {

        private TestFileFormat(String formatIdentifier) {
            super(formatIdentifier);
        }

        @Override
        public FormatReaderFactory createReaderFactory(
                RowType dataSchemaRowType,
                RowType projectedRowType,
                @Nullable List<Predicate> filters) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FormatWriterFactory createWriterFactory(RowType type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void validateDataFields(RowType rowType) {}

        @Override
        public Optional<SimpleStatsExtractor> createStatsExtractor(
                RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
            return Optional.empty();
        }
    }
}
