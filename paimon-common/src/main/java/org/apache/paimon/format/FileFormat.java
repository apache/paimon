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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.factories.FormatFactoryUtil;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.normalizeFileFormat;

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
     * @param dataSchemaRowType The full table schema type.
     * @param projectedRowType Type with projection.
     * @param filters A list of filters in conjunctive form for filtering on a best-effort basis.
     */
    public abstract FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType, RowType projectedRowType, @Nullable List<Predicate> filters);

    /** Create a {@link FormatWriterFactory} from the type. */
    public abstract FormatWriterFactory createWriterFactory(RowType type);

    /** Validate data field type supported or not. */
    public abstract void validateDataFields(RowType rowType);

    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.empty();
    }

    public static FileFormat fromIdentifier(String identifier, Options options) {
        return fromIdentifier(identifier, options, FileFormatProvider.FORMAT_PROVIDER);
    }

    public static FileFormat readerFromIdentifier(String identifier, Options options) {
        return fromIdentifier(identifier, options, FileFormatProvider.READ_FORMAT_PROVIDER);
    }

    public static FileFormat writerFromIdentifier(String identifier, Options options) {
        return fromIdentifier(identifier, options, FileFormatProvider.WRITE_FORMAT_PROVIDER);
    }

    public static FileFormat validationFromIdentifier(String identifier, Options options) {
        return fromIdentifier(identifier, options, FileFormatProvider.VALIDATION_FORMAT_PROVIDER);
    }

    private static FileFormat fromIdentifier(
            String identifier, Options options, String providerOptionKey) {
        return fromIdentifier(
                normalizeFileFormat(identifier),
                new FormatContext(
                        options,
                        options.get(CoreOptions.READ_BATCH_SIZE),
                        options.get(CoreOptions.WRITE_BATCH_SIZE),
                        options.get(CoreOptions.WRITE_BATCH_MEMORY),
                        options.get(CoreOptions.FILE_COMPRESSION_ZSTD_LEVEL),
                        options.get(CoreOptions.FILE_BLOCK_SIZE)),
                providerOptionKey);
    }

    /** Create a {@link FileFormat} from format identifier and format options. */
    public static FileFormat fromIdentifier(String identifier, FormatContext context) {
        return fromIdentifier(identifier, context, FileFormatProvider.FORMAT_PROVIDER);
    }

    public static FileFormat readerFromIdentifier(String identifier, FormatContext context) {
        return fromIdentifier(identifier, context, FileFormatProvider.READ_FORMAT_PROVIDER);
    }

    public static FileFormat writerFromIdentifier(String identifier, FormatContext context) {
        return fromIdentifier(identifier, context, FileFormatProvider.WRITE_FORMAT_PROVIDER);
    }

    public static FileFormat validationFromIdentifier(String identifier, FormatContext context) {
        return fromIdentifier(identifier, context, FileFormatProvider.VALIDATION_FORMAT_PROVIDER);
    }

    private static FileFormat fromIdentifier(
            String identifier, FormatContext context, String providerOptionKey) {
        String normalizedIdentifier = normalizeFileFormat(identifier);
        ClassLoader classLoader = providerClassLoader();
        Optional<FileFormat> operationFormat =
                createFromProvider(classLoader, context, providerOptionKey, normalizedIdentifier);
        if (operationFormat.isPresent()) {
            return operationFormat.get();
        }

        if (!FileFormatProvider.FORMAT_PROVIDER.equals(providerOptionKey)) {
            Optional<FileFormat> genericFormat =
                    createFromProvider(
                            classLoader,
                            context,
                            FileFormatProvider.FORMAT_PROVIDER,
                            normalizedIdentifier);
            if (genericFormat.isPresent()) {
                return genericFormat.get();
            }
        }

        return FormatFactoryUtil.discoverFactory(
                        FileFormat.class.getClassLoader(), normalizedIdentifier)
                .create(context);
    }

    private static Optional<FileFormat> createFromProvider(
            ClassLoader classLoader,
            FormatContext context,
            String providerOptionKey,
            String normalizedIdentifier) {
        String providerIdentifier = context.options().getString(providerOptionKey, null);
        if (providerIdentifier == null || providerIdentifier.trim().isEmpty()) {
            return Optional.empty();
        }
        return FormatFactoryUtil.discoverProvider(
                        classLoader, providerIdentifier.trim().toLowerCase(Locale.ROOT))
                .create(normalizedIdentifier, context);
    }

    private static ClassLoader providerClassLoader() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader != null) {
            try {
                if (contextClassLoader.loadClass(FileFormatProvider.class.getName())
                        == FileFormatProvider.class) {
                    return contextClassLoader;
                }
            } catch (ClassNotFoundException ignored) {
                // Fall back to the class loader that loaded Paimon's format API.
            }
        }
        return FileFormat.class.getClassLoader();
    }

    protected Options getIdentifierPrefixOptions(Options options) {
        Map<String, String> result = new HashMap<>();
        String prefix = formatIdentifier.toLowerCase(Locale.ROOT) + ".";
        for (String key : options.keySet()) {
            if (key.toLowerCase(Locale.ROOT).startsWith(prefix)) {
                result.put(prefix + key.substring(prefix.length()), options.get(key));
            }
        }
        return new Options(result);
    }

    public static FileFormat fileFormat(CoreOptions options) {
        return FileFormat.fromIdentifier(options.fileFormatString(), options.toConfiguration());
    }

    public static FileFormat readerFileFormat(CoreOptions options) {
        return FileFormat.readerFromIdentifier(
                options.fileFormatString(), options.toConfiguration());
    }

    public static FileFormat writerFileFormat(CoreOptions options) {
        return FileFormat.writerFromIdentifier(
                options.fileFormatString(), options.toConfiguration());
    }

    public static FileFormat validationFileFormat(CoreOptions options) {
        return FileFormat.validationFromIdentifier(
                options.fileFormatString(), options.toConfiguration());
    }

    @Nullable
    public static FileFormat vectorFileFormat(CoreOptions options) {
        String vectorFileFormat = options.vectorFileFormatString();
        if (vectorFileFormat == null) {
            return null;
        }
        return FileFormat.writerFromIdentifier(vectorFileFormat, options.toConfiguration());
    }

    @Nullable
    public static FileFormat validationVectorFileFormat(CoreOptions options) {
        String vectorFileFormat = options.vectorFileFormatString();
        if (vectorFileFormat == null) {
            return null;
        }
        return FileFormat.validationFromIdentifier(vectorFileFormat, options.toConfiguration());
    }

    public static FileFormat manifestFormat(CoreOptions options) {
        return FileFormat.fromIdentifier(options.manifestFormatString(), options.toConfiguration());
    }
}
