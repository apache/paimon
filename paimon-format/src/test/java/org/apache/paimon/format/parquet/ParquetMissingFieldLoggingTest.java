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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests diagnostics for physical columns supplied by an upper-layer reader. */
class ParquetMissingFieldLoggingTest {

    private static final DataField ID = new DataField(0, "id", DataTypes.INT());
    private static final RowType READ_TYPE =
            new RowType(
                    Arrays.asList(
                            ID,
                            SpecialFields.ROW_ID.copy(true),
                            SpecialFields.SEQUENCE_NUMBER.copy(true)));
    private static final Set<String> TRACKING_FIELDS =
            new HashSet<>(Arrays.asList("_ROW_ID", "_SEQUENCE_NUMBER"));

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testFallbackFieldsDoNotWarnButOtherMissingFieldsDo() throws Exception {
        Path path = writeFile();
        RowType readType =
                new RowType(
                        Arrays.asList(
                                ID,
                                SpecialFields.ROW_ID.copy(true),
                                SpecialFields.SEQUENCE_NUMBER.copy(true),
                                new DataField(1, "missing", DataTypes.STRING())));
        ParquetReaderFactory factory = new ParquetReaderFactory(new Options(), readType, 10, null);
        try (Warnings warnings = new Warnings()) {
            read(factory, path, TRACKING_FIELDS, 4);
            assertThat(warnings.messages).hasSize(1);
            assertThat(warnings.messages.get(0)).startsWith("missing does not exist in ");
        }
    }

    @Test
    void testPruningWarnsForMissingFilterField() throws Exception {
        Path path = writeFile();
        RowType readType =
                new RowType(
                        Arrays.asList(
                                ID,
                                new DataField(1, "price", DataTypes.INT()),
                                SpecialFields.ROW_ID.copy(true)));
        Predicate filter = new PredicateBuilder(readType).greaterThan(1, 0);
        ParquetReaderFactory factory =
                new ParquetReaderFactory(
                        new Options(), readType, 10, Collections.singletonList(filter));
        LocalFileIO io = new LocalFileIO();
        FormatReaderContext context =
                new FormatReaderContext(io, path, io.getFileSize(path), null, null)
                        .withFallbackFields(TRACKING_FIELDS);

        try (Warnings warnings = new Warnings()) {
            assertThat(factory.candidateRowRanges(context)).isEmpty();
            assertThat(warnings.messages).hasSize(1);
            assertThat(warnings.messages.get(0)).startsWith("price does not exist in ");

            try (RecordReader<InternalRow> reader = factory.createReader(context)) {
                assertThat(reader.readBatch()).isNull();
            }
            assertThat(warnings.messages).hasSize(1);
        }
    }

    @Test
    void testSchemaCacheDoesNotHideWarningsWithoutFallback() throws Exception {
        Path path = writeFile();
        ParquetReaderFactory factory = new ParquetReaderFactory(new Options(), READ_TYPE, 10, null);
        try (Warnings warnings = new Warnings()) {
            read(factory, path, TRACKING_FIELDS, 3);
            assertThat(warnings.messages).isEmpty();
            // The same cached schema can also be read without an upper-layer fallback.
            read(factory, path, Collections.emptySet(), 3);
            assertThat(warnings.messages).hasSize(2);
            assertThat(warnings.messages.get(0)).startsWith("_ROW_ID does not exist in ");
            assertThat(warnings.messages.get(1)).startsWith("_SEQUENCE_NUMBER does not exist in ");
            read(factory, path, Collections.emptySet(), 3);
            assertThat(warnings.messages).hasSize(2);
            assertThat(factory.requestedSchemaCache()).hasSize(1);
        }
    }

    @Test
    void testFallbackIsSpecificToEachField() throws Exception {
        Path path = writeFile();
        ParquetReaderFactory factory = new ParquetReaderFactory(new Options(), READ_TYPE, 10, null);
        try (Warnings warnings = new Warnings()) {
            read(factory, path, Collections.singleton("_SEQUENCE_NUMBER"), 3);
            assertThat(warnings.messages).hasSize(1);
            assertThat(warnings.messages.get(0)).startsWith("_ROW_ID does not exist in ");
        }
    }

    private Path writeFile() throws Exception {
        Path path = new Path(tempDir.toString(), "data.parquet");
        RowType type = new RowType(Collections.singletonList(ID));
        ParquetWriterFactory factory =
                new ParquetWriterFactory(new RowDataParquetBuilder(type, new Options()));
        try (FormatWriter writer =
                factory.create(new LocalFileIO().newOutputStream(path, false), "snappy")) {
            writer.addElement(GenericRow.of(7));
        }
        return path;
    }

    private void read(ParquetReaderFactory factory, Path path, Set<String> fallback, int fieldCount)
            throws Exception {
        LocalFileIO io = new LocalFileIO();
        FormatReaderContext context =
                new FormatReaderContext(io, path, io.getFileSize(path), null, null)
                        .withFallbackFields(fallback);
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = factory.createReader(context)) {
            reader.forEachRemaining(
                    row -> {
                        ids.add(row.getInt(0));
                        // Declaring a fallback must not change the physical reader's null filling.
                        for (int i = 1; i < fieldCount; i++) {
                            assertThat(row.isNullAt(i)).isTrue();
                        }
                    });
        }
        assertThat(ids).containsExactly(7);
    }

    private static class Warnings extends AbstractAppender implements AutoCloseable {
        private final Logger logger = (Logger) LogManager.getLogger(ParquetReaderFactory.class);
        private final Level previousLevel = logger.getLevel();
        private final List<String> messages = new ArrayList<>();

        private Warnings() {
            super(
                    "missing-field-warnings",
                    null,
                    PatternLayout.createDefaultLayout(),
                    false,
                    Property.EMPTY_ARRAY);
            start();
            logger.addAppender(this);
            logger.setLevel(Level.WARN);
        }

        @Override
        public void append(LogEvent event) {
            if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        }

        @Override
        public void close() {
            logger.removeAppender(this);
            logger.setLevel(previousLevel);
            stop();
        }
    }
}
