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

package org.apache.paimon.format;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;

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
     * @param filters A list of filters in conjunctive form for filtering on a best-effort basis.
     */
    public abstract FormatReaderFactory createReaderFactory(
            RowType type, int[][] projection, @Nullable List<Predicate> filters);

    /** Create a {@link FormatWriterFactory} from the type. */
    public abstract FormatWriterFactory createWriterFactory(RowType type);

    /** Validate data field type supported or not. */
    public abstract void validateDataFields(RowType rowType);

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

    public Optional<Function<ColumnStatisticsCollectSkipper, FileStatsExtractor>>
            createStatsExtractorSupplier(RowType type) {
        return Optional.empty();
    }

    @VisibleForTesting
    public static FileFormat fromIdentifier(String identifier, Options options) {
        return fromIdentifier(identifier, new FormatContext(options, 1024));
    }

    /** Create a {@link FileFormat} from format identifier and format options. */
    public static FileFormat fromIdentifier(String identifier, FormatContext context) {
        Optional<FileFormat> format =
                fromIdentifier(identifier, context, Thread.currentThread().getContextClassLoader());
        return format.orElseGet(
                () ->
                        fromIdentifier(identifier, context, FileFormat.class.getClassLoader())
                                .orElseThrow(
                                        () ->
                                                new RuntimeException(
                                                        String.format(
                                                                "Could not find any factories that implement '%s' in the classpath.",
                                                                FileFormatFactory.class
                                                                        .getName()))));
    }

    private static Optional<FileFormat> fromIdentifier(
            String formatIdentifier, FormatContext context, ClassLoader classLoader) {
        ServiceLoader<FileFormatFactory> serviceLoader =
                ServiceLoader.load(FileFormatFactory.class, classLoader);
        for (FileFormatFactory factory : serviceLoader) {
            if (factory.identifier().equals(formatIdentifier.toLowerCase())) {
                return Optional.of(factory.create(context));
            }
        }

        return Optional.empty();
    }
}
