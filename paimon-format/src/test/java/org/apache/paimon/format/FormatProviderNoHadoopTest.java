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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that {@link FileFormatProvider} can bypass Hadoop-backed format constructors. */
public class FormatProviderNoHadoopTest {

    private static final String TEST_PROVIDER = "format-test-provider";

    @Test
    public void testProviderBypassesHadoopBackedOrcAndParquetFormatPaths() throws Exception {
        try (URLClassLoader classLoader = new NoHadoopClassLoader(testClasspathWithoutHadoop())) {
            Class<?> runner =
                    Class.forName(NoHadoopFormatProviderRunner.class.getName(), true, classLoader);
            runner.getMethod("run", ClassLoader.class).invoke(null, classLoader);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw (Error) cause;
        }
    }

    private static URL[] testClasspathWithoutHadoop() {
        return Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
                .filter(path -> !path.contains("/hadoop-"))
                .filter(path -> !path.contains("/htrace-core"))
                .filter(path -> !path.contains("/woodstox-core"))
                .filter(path -> !path.contains("/stax2-api"))
                .map(
                        path -> {
                            try {
                                return new File(path).toURI().toURL();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .toArray(URL[]::new);
    }

    private static class NoHadoopClassLoader extends URLClassLoader {

        private NoHadoopClassLoader(URL[] urls) {
            super(urls, ClassLoader.getSystemClassLoader().getParent());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("org.apache.hadoop.")) {
                throw new ClassNotFoundException(name);
            }
            return super.loadClass(name, resolve);
        }
    }

    /** Runner loaded by {@link NoHadoopClassLoader}. */
    public static class NoHadoopFormatProviderRunner {

        public static void run(ClassLoader classLoader) throws Exception {
            assertThatThrownBy(() -> classLoader.loadClass("org.apache.hadoop.conf.Configuration"))
                    .isInstanceOf(ClassNotFoundException.class);

            RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));

            Options options = new Options();
            options.setString(FileFormatProvider.WRITE_FORMAT_PROVIDER, TEST_PROVIDER);
            options.setString(FileFormatProvider.READ_FORMAT_PROVIDER, TEST_PROVIDER);
            options.setString(FileFormatProvider.VALIDATION_FORMAT_PROVIDER, TEST_PROVIDER);

            FileFormat orc = FileFormat.writerFromIdentifier("orc", options);
            FileFormat parquet = FileFormat.writerFromIdentifier("parquet", options);
            FileFormat orcReader = FileFormat.readerFromIdentifier("orc", options);
            FileFormat orcValidation = FileFormat.validationFromIdentifier("orc", options);

            assertThat(orc).isInstanceOf(TestFileFormat.class);
            assertThat(parquet).isInstanceOf(TestFileFormat.class);
            assertThat(orcReader).isInstanceOf(TestFileFormat.class);
            assertThat(orcValidation).isInstanceOf(TestFileFormat.class);
            assertThat(orc.createWriterFactory(rowType))
                    .isInstanceOf(TestFormatWriterFactory.class);
            assertThat(parquet.createWriterFactory(rowType))
                    .isInstanceOf(TestFormatWriterFactory.class);
            assertThat(orcReader.createReaderFactory(rowType, rowType, null))
                    .isInstanceOf(TestFormatReaderFactory.class);
            assertThatCode(() -> orcValidation.validateDataFields(rowType))
                    .doesNotThrowAnyException();

            Options writerOnlyOptions = new Options();
            writerOnlyOptions.setString(FileFormatProvider.WRITE_FORMAT_PROVIDER, TEST_PROVIDER);
            assertThatThrownBy(() -> FileFormat.readerFromIdentifier("orc", writerOnlyOptions))
                    .hasRootCauseInstanceOf(ClassNotFoundException.class);

            Options writerAndGenericOptions = new Options();
            writerAndGenericOptions.setString(
                    FileFormatProvider.WRITE_FORMAT_PROVIDER, TEST_PROVIDER);
            writerAndGenericOptions.setString(FileFormatProvider.FORMAT_PROVIDER, TEST_PROVIDER);
            assertThat(FileFormat.readerFromIdentifier("orc", writerAndGenericOptions))
                    .isInstanceOf(TestFileFormat.class);
        }
    }

    /** Test provider used by the no-Hadoop classloader runner. */
    public static class TestFileFormatProvider implements FileFormatProvider {

        @Override
        public String identifier() {
            return TEST_PROVIDER;
        }

        @Override
        public Optional<FileFormat> create(String identifier, FormatContext context) {
            if ("orc".equals(identifier) || "parquet".equals(identifier)) {
                return Optional.of(new TestFileFormat(identifier));
            }
            return Optional.empty();
        }
    }

    /** Test format implementation that avoids Hadoop-backed constructors. */
    public static class TestFileFormat extends FileFormat {

        private TestFileFormat(String formatIdentifier) {
            super(formatIdentifier);
        }

        @Override
        public FormatReaderFactory createReaderFactory(
                RowType dataSchemaRowType,
                RowType projectedRowType,
                @Nullable List<Predicate> filters) {
            return new TestFormatReaderFactory();
        }

        @Override
        public FormatWriterFactory createWriterFactory(RowType type) {
            return new TestFormatWriterFactory();
        }

        @Override
        public void validateDataFields(RowType rowType) {}

        @Override
        public Optional<SimpleStatsExtractor> createStatsExtractor(
                RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
            return Optional.empty();
        }
    }

    /** Test reader factory returned by {@link TestFileFormat}. */
    public static class TestFormatReaderFactory implements FormatReaderFactory {

        @Override
        public FileRecordReader<InternalRow> createReader(Context context) {
            throw new UnsupportedOperationException();
        }
    }

    /** Test writer factory returned by {@link TestFileFormat}. */
    public static class TestFormatWriterFactory implements FormatWriterFactory {

        @Override
        public FormatWriter create(PositionOutputStream out, String compression) {
            return new TestFormatWriter();
        }
    }

    private static class TestFormatWriter implements FormatWriter {

        @Override
        public void addElement(InternalRow element) {}

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
            return false;
        }

        @Override
        public void close() {}
    }
}
