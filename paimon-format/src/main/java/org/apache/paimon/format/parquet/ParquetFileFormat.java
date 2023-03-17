/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.parquet;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileStatsExtractor;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.format.parquet.ParquetFileFormatFactory.IDENTIFIER;

/** Parquet {@link FileFormat}. */
public class ParquetFileFormat extends FileFormat {

    private final Options formatOptions;

    public ParquetFileFormat(Options formatOptions) {
        super(IDENTIFIER);
        this.formatOptions = formatOptions;
    }

    @VisibleForTesting
    Options formatOptions() {
        return formatOptions;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType type, int[][] projection, List<Predicate> filters) {
        return new ParquetReaderFactory(
                getParquetConfiguration(formatOptions), Projection.of(projection).project(type));
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new ParquetWriterFactory(
                new RowDataParquetBuilder(type, getParquetConfiguration(formatOptions)));
    }

    @Override
    public Optional<FileStatsExtractor> createStatsExtractor(RowType type) {
        return Optional.of(new ParquetFileStatsExtractor(type));
    }

    public static Options getParquetConfiguration(Options options) {
        Options conf = new Options();
        options.toMap().forEach((key, value) -> conf.setString(IDENTIFIER + "." + key, value));
        return conf;
    }
}
