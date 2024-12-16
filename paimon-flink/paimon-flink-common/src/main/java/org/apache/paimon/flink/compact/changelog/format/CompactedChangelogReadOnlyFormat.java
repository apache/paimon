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

package org.apache.paimon.flink.compact.changelog.format;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** {@link FileFormat} for compacted changelog. */
public class CompactedChangelogReadOnlyFormat extends FileFormat {

    private final FileFormat wrapped;

    protected CompactedChangelogReadOnlyFormat(String formatIdentifier, FileFormat wrapped) {
        super(formatIdentifier);
        this.wrapped = wrapped;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType projectedRowType, @Nullable List<Predicate> filters) {
        return new CompactedChangelogFormatReaderFactory(
                wrapped.createReaderFactory(projectedRowType, filters));
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void validateDataFields(RowType rowType) {
        wrapped.validateDataFields(rowType);
    }

    public static String getIdentifier(String wrappedFormat) {
        return "cc-" + wrappedFormat;
    }

    static class AbstractFactory implements FileFormatFactory {

        private final String format;

        AbstractFactory(String format) {
            this.format = format;
        }

        @Override
        public String identifier() {
            return getIdentifier(format);
        }

        @Override
        public FileFormat create(FormatContext formatContext) {
            return new CompactedChangelogReadOnlyFormat(
                    getIdentifier(format), FileFormat.fromIdentifier(format, formatContext));
        }
    }

    /** {@link FileFormatFactory} for compacted changelog, with orc as the real format. */
    public static class OrcFactory extends AbstractFactory {

        public OrcFactory() {
            super("orc");
        }
    }

    /** {@link FileFormatFactory} for compacted changelog, with parquet as the real format. */
    public static class ParquetFactory extends AbstractFactory {

        public ParquetFactory() {
            super("parquet");
        }
    }

    /** {@link FileFormatFactory} for compacted changelog, with avro as the real format. */
    public static class AvroFactory extends AbstractFactory {

        public AvroFactory() {
            super("avro");
        }
    }
}
