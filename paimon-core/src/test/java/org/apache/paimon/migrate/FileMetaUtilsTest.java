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

package org.apache.paimon.migrate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FileFormatProvider;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link FileMetaUtils}. */
public class FileMetaUtilsTest {

    private static final String READ_PROVIDER = "file-meta-read-provider";
    private static final String PROVIDER_FORMAT = "file-meta-provider-format";
    private static final RowType ROW_TYPE = RowType.of(DataTypes.INT());

    @Test
    public void testCreateSimpleStatsExtractorUsesReadProvider() {
        Options options = new Options();
        options.set(FileFormatProvider.READ_FORMAT_PROVIDER, READ_PROVIDER);

        FileStoreTable table = mock(FileStoreTable.class);
        when(table.coreOptions()).thenReturn(new CoreOptions(options));
        when(table.rowType()).thenReturn(ROW_TYPE);

        SimpleStatsExtractor extractor =
                FileMetaUtils.createSimpleStatsExtractor(table, PROVIDER_FORMAT);

        assertThat(options.toMap()).doesNotContainKey(FileFormatProvider.FORMAT_PROVIDER);
        assertThat(extractor).isInstanceOf(ReadStatsExtractor.class);
    }

    /** Test provider selected only for external-file stats reads. */
    public static class ReadStatsFileFormatProvider implements FileFormatProvider {

        @Override
        public String identifier() {
            return READ_PROVIDER;
        }

        @Override
        public Optional<FileFormat> create(String identifier, FormatContext context) {
            if (PROVIDER_FORMAT.equals(identifier)) {
                return Optional.of(new ReadStatsFileFormat(identifier));
            }
            return Optional.empty();
        }
    }

    private static class ReadStatsFileFormat extends FileFormat {

        private ReadStatsFileFormat(String formatIdentifier) {
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
            return Optional.of(new ReadStatsExtractor());
        }
    }

    private static class ReadStatsExtractor implements SimpleStatsExtractor {

        @Override
        public SimpleColStats[] extract(FileIO fileIO, Path path, long length) throws IOException {
            return new SimpleColStats[0];
        }

        @Override
        public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(
                FileIO fileIO, Path path, long length) throws IOException {
            return Pair.of(new SimpleColStats[0], new FileInfo(0));
        }
    }
}
