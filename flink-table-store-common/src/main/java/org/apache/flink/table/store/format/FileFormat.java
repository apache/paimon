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

package org.apache.flink.table.store.format;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Factory class which creates reader and writer factories for specific file format.
 *
 * <p>NOTE: This class must be thread safe.
 */
public abstract class FileFormat {

    protected String formatIdentifier;

    protected FileFormat(String formatIdentifier) {
        this.formatIdentifier = formatIdentifier;
    }

    public String getFormatIdentifier() {
        return formatIdentifier;
    }

    /**
     * Create a {@link FormatReaderFactory} from the type, with projection pushed down.
     *
     * @param type Type without projection.
     * @param projection See {@link org.apache.flink.table.connector.Projection#toNestedIndexes()}.
     * @param filters A list of filters in conjunctive form for filtering on a best-effort basis.
     */
    public abstract FormatReaderFactory createReaderFactory(
            RowType type, int[][] projection, @Nullable List<Predicate> filters);

    /** Create a {@link FormatWriterFactory} from the type. */
    public abstract FormatWriterFactory createWriterFactory(RowType type);

    public FormatReaderFactory createReaderFactory(RowType rowType) {
        int[][] projection = new int[rowType.getFieldCount()][];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = new int[] {i};
        }
        return createReaderFactory(rowType, projection);
    }

    public FormatReaderFactory createReaderFactory(RowType rowType, int[][] projection) {
        return createReaderFactory(rowType, projection, new ArrayList<>());
    }

    public Optional<FileStatsExtractor> createStatsExtractor(RowType type) {
        return Optional.empty();
    }

    /** Create a {@link FileFormat} from table options. */
    public static FileFormat fromTableOptions(
            Configuration tableOptions, ConfigOption<String> formatOption) {
        String formatIdentifier = tableOptions.get(formatOption);
        DelegatingConfiguration formatOptions =
                new DelegatingConfiguration(tableOptions, formatIdentifier + ".");
        return fromIdentifier(formatIdentifier, formatOptions);
    }

    /** Create a {@link FileFormat} from format identifier and format options. */
    public static FileFormat fromIdentifier(String identifier, Configuration options) {
        Optional<FileFormat> format =
                fromIdentifier(identifier, options, Thread.currentThread().getContextClassLoader());
        return format.orElseGet(
                () ->
                        fromIdentifier(identifier, options, FileFormat.class.getClassLoader())
                                .orElseThrow(
                                        () ->
                                                new RuntimeException(
                                                        String.format(
                                                                "Could not find any factories that implement '%s' in the classpath.",
                                                                FileFormatFactory.class
                                                                        .getName()))));
    }

    private static Optional<FileFormat> fromIdentifier(
            String formatIdentifier, Configuration formatOptions, ClassLoader classLoader) {
        ServiceLoader<FileFormatFactory> serviceLoader =
                ServiceLoader.load(FileFormatFactory.class, classLoader);
        for (FileFormatFactory factory : serviceLoader) {
            if (factory.identifier().equals(formatIdentifier.toLowerCase())) {
                return Optional.of(factory.create(formatOptions));
            }
        }

        return Optional.empty();
    }
}
