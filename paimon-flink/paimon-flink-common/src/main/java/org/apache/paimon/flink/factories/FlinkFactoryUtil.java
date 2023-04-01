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

package org.apache.paimon.flink.factories;

import org.apache.paimon.flink.log.LogStoreTableFactory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.factories.DecodingFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.EncodingFormatFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FormatFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.configuration.ConfigurationUtils.canBePrefixMap;
import static org.apache.flink.configuration.ConfigurationUtils.filterPrefixMapKey;
import static org.apache.flink.table.factories.ManagedTableFactory.DEFAULT_IDENTIFIER;

/** Utility for working with {@link Factory}s. */
public final class FlinkFactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkFactoryUtil.class);

    public static final ConfigOption<String> FORMAT =
            ConfigOptions.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding data. "
                                    + "The identifier is used to discover a suitable format factory.");

    /**
     * Suffix for keys of {@link ConfigOption} in case a connector requires multiple formats (e.g.
     * for both key and value).
     *
     * <p>See {@link #createFlinkTableFactoryHelper(LogStoreTableFactory, Context)} Context)} for
     * more information.
     */
    public static final String FORMAT_SUFFIX = ".format";

    /**
     * Creates a utility that helps in discovering formats, merging options with {@link
     * DynamicTableFactory.Context#getEnrichmentOptions()} and validating them all for a {@link
     * LogStoreTableFactory}.
     *
     * <p>The following example sketches the usage:
     *
     * <pre>{@code
     * // in createDynamicTableSource()
     * helper = FlinkFactoryUtil.createFlinkTableFactoryHelper(this, context);
     *
     * keyFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, KEY_FORMAT);
     * valueFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, VALUE_FORMAT);
     *
     * helper.validate();
     *
     * ... // construct connector with discovered formats
     * }</pre>
     */
    public static FlinkTableFactoryHelper createFlinkTableFactoryHelper(
            LogStoreTableFactory factory, DynamicTableFactory.Context context) {
        return new FlinkTableFactoryHelper(factory, context);
    }

    /** Discovers a flink Factory using the given factory base class and identifier. */
    @SuppressWarnings("unchecked")
    public static <T extends Factory> T discoverFlinkFactory(
            ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        final List<Factory> factories = discoverFlinkFactories(classLoader);

        final List<Factory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        if (foundFactories.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any factories that implement '%s' in the classpath.",
                            factoryClass.getName()));
        }

        final List<Factory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equals(factoryIdentifier))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            foundFactories.stream()
                                    .map(Factory::factoryIdentifier)
                                    .filter(identifier -> !DEFAULT_IDENTIFIER.equals(identifier))
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingFactories.size() > 1) {
            throw new ValidationException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return (T) matchingFactories.get(0);
    }

    /** Returns the required option prefix for options of the given format. */
    public static String getFormatPrefix(
            ConfigOption<String> formatOption, String formatIdentifier) {
        final String formatOptionKey = formatOption.key();
        if (formatOptionKey.equals(FORMAT.key())) {
            return formatIdentifier + ".";
        } else if (formatOptionKey.endsWith(FORMAT_SUFFIX)) {
            // extract the key prefix, e.g. extract 'key' from 'key.format'
            String keyPrefix =
                    formatOptionKey.substring(0, formatOptionKey.length() - FORMAT_SUFFIX.length());
            return keyPrefix + "." + formatIdentifier + ".";
        } else {
            throw new ValidationException(
                    "Format identifier key should be 'format' or suffix with '.format', "
                            + "don't support format identifier key '"
                            + formatOptionKey
                            + "'.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    static List<Factory> discoverFlinkFactories(ClassLoader classLoader) {
        final Iterator<Factory> serviceLoaderIterator =
                ServiceLoader.load(Factory.class, classLoader).iterator();

        final List<Factory> loadResults = new ArrayList<>();
        while (true) {
            try {
                // error handling should also be applied to the hasNext() call because service
                // loading might cause problems here as well
                if (!serviceLoaderIterator.hasNext()) {
                    break;
                }

                loadResults.add(serviceLoaderIterator.next());
            } catch (Throwable t) {
                if (t instanceof NoClassDefFoundError) {
                    LOG.debug(
                            "NoClassDefFoundError when loading a "
                                    + LogStoreTableFactory.class.getCanonicalName()
                                    + ". This is expected when trying to load a format dependency but no flink-connector-files is loaded.",
                            t);
                } else {
                    throw new TableException(
                            "Unexpected error when trying to load service provider.", t);
                }
            }
        }

        return loadResults;
    }

    private static Set<String> allKeysExpanded(ConfigOption<?> option, Set<String> actualKeys) {
        return allKeysExpanded("", option, actualKeys);
    }

    private static Set<String> allKeysExpanded(
            String prefix, ConfigOption<?> option, Set<String> actualKeys) {
        final Set<String> staticKeys =
                allKeys(option).map(k -> prefix + k).collect(Collectors.toSet());
        if (!canBePrefixMap(option)) {
            return staticKeys;
        }
        // include all prefix keys of a map option by considering the actually provided keys
        return Stream.concat(
                        staticKeys.stream(),
                        staticKeys.stream()
                                .flatMap(
                                        k ->
                                                actualKeys.stream()
                                                        .filter(c -> filterPrefixMapKey(k, c))))
                .collect(Collectors.toSet());
    }

    private static Stream<String> allKeys(ConfigOption<?> option) {
        return Stream.concat(Stream.of(option.key()), fallbackKeys(option));
    }

    private static Stream<String> fallbackKeys(ConfigOption<?> option) {
        return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                .map(FallbackKey::getKey);
    }

    private static Stream<String> deprecatedKeys(ConfigOption<?> option) {
        return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                .filter(FallbackKey::isDeprecated)
                .map(FallbackKey::getKey);
    }

    /** Base flink helper utility for validating all options for a {@link LogStoreTableFactory}. */
    public static class FlinkFactoryHelper<F extends LogStoreTableFactory> {

        protected final F factory;

        protected final Configuration allOptions;

        protected final Set<String> consumedOptionKeys;

        protected final Set<String> deprecatedOptionKeys;

        public FlinkFactoryHelper(
                F factory, Map<String, String> configuration, ConfigOption<?>... implicitOptions) {
            this.factory = factory;
            this.allOptions = Configuration.fromMap(configuration);

            final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
            consumedOptions.addAll(Arrays.asList(implicitOptions));

            consumedOptionKeys =
                    consumedOptions.stream()
                            .flatMap(
                                    option -> allKeysExpanded(option, allOptions.keySet()).stream())
                            .collect(Collectors.toSet());

            deprecatedOptionKeys =
                    consumedOptions.stream()
                            .flatMap(FlinkFactoryUtil::deprecatedKeys)
                            .collect(Collectors.toSet());
        }

        /** Returns all options currently being consumed by the factory. */
        public ReadableConfig getOptions() {
            return allOptions;
        }
    }

    /**
     * Helper utility for discovering formats and validating all options for a {@link
     * DynamicTableFactory}.
     *
     * @see #createFlinkTableFactoryHelper(LogStoreTableFactory, Context)
     */
    public static class FlinkTableFactoryHelper extends FlinkFactoryHelper<LogStoreTableFactory> {

        private final Context context;

        private final Configuration enrichingOptions;

        private FlinkTableFactoryHelper(LogStoreTableFactory tableFactory, Context context) {
            super(tableFactory, context.getCatalogTable().getOptions());
            this.context = context;
            this.enrichingOptions = Configuration.fromMap(context.getEnrichmentOptions());
        }

        /**
         * Returns all options currently being consumed by the factory. This method returns the
         * options already merged with {@link Context#getEnrichmentOptions()}, using {@link
         * DynamicTableFactory#forwardOptions()} as reference of mergeable options.
         */
        @Override
        public ReadableConfig getOptions() {
            return super.getOptions();
        }

        /**
         * Discovers a {@link DecodingFormat} of the given type using the given option as factory
         * identifier.
         */
        public <I, F extends DecodingFormatFactory<I>> DecodingFormat<I> discoverDecodingFormat(
                Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalDecodingFormat(formatFactoryClass, formatOption)
                    .orElseThrow(
                            () ->
                                    new ValidationException(
                                            String.format(
                                                    "Could not find required scan format '%s'.",
                                                    formatOption.key())));
        }

        /**
         * Discovers a {@link DecodingFormat} of the given type using the given option (if present)
         * as factory identifier.
         */
        public <I, F extends DecodingFormatFactory<I>>
                Optional<DecodingFormat<I>> discoverOptionalDecodingFormat(
                        Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalFormatFactory(formatFactoryClass, formatOption)
                    .map(
                            formatFactory -> {
                                String formatPrefix =
                                        formatFlinkPrefix(formatFactory, formatOption);
                                try {
                                    return formatFactory.createDecodingFormat(
                                            context,
                                            createFormatOptions(formatPrefix, formatFactory));
                                } catch (Throwable t) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Error creating scan format '%s' in option space '%s'.",
                                                    formatFactory.factoryIdentifier(),
                                                    formatPrefix),
                                            t);
                                }
                            });
        }

        /**
         * Discovers a {@link EncodingFormat} of the given type using the given option as factory
         * identifier.
         */
        public <I, F extends EncodingFormatFactory<I>> EncodingFormat<I> discoverEncodingFormat(
                Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalEncodingFormat(formatFactoryClass, formatOption)
                    .orElseThrow(
                            () ->
                                    new ValidationException(
                                            String.format(
                                                    "Could not find required sink format '%s'.",
                                                    formatOption.key())));
        }

        /**
         * Discovers a {@link EncodingFormat} of the given type using the given option (if present)
         * as factory identifier.
         */
        public <I, F extends EncodingFormatFactory<I>>
                Optional<EncodingFormat<I>> discoverOptionalEncodingFormat(
                        Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalFormatFactory(formatFactoryClass, formatOption)
                    .map(
                            formatFactory -> {
                                String formatPrefix =
                                        formatFlinkPrefix(formatFactory, formatOption);
                                try {
                                    return formatFactory.createEncodingFormat(
                                            context,
                                            createFormatOptions(formatPrefix, formatFactory));
                                } catch (Throwable t) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Error creating sink format '%s' in option space '%s'.",
                                                    formatFactory.factoryIdentifier(),
                                                    formatPrefix),
                                            t);
                                }
                            });
        }

        // ----------------------------------------------------------------------------------------

        private <F extends Factory> Optional<F> discoverOptionalFormatFactory(
                Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            final String identifier = allOptions.get(formatOption);
            checkFormatIdentifierMatchesWithEnrichingOptions(formatOption, identifier);
            if (identifier == null) {
                return Optional.empty();
            }
            final F factory =
                    discoverFlinkFactory(context.getClassLoader(), formatFactoryClass, identifier);
            String formatPrefix = formatFlinkPrefix(factory, formatOption);

            // log all used options of other factories
            final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
            consumedOptions.addAll(factory.requiredOptions());
            consumedOptions.addAll(factory.optionalOptions());

            consumedOptions.stream()
                    .flatMap(
                            option ->
                                    allKeysExpanded(formatPrefix, option, allOptions.keySet())
                                            .stream())
                    .forEach(consumedOptionKeys::add);

            consumedOptions.stream()
                    .flatMap(FlinkFactoryUtil::deprecatedKeys)
                    .map(k -> formatPrefix + k)
                    .forEach(deprecatedOptionKeys::add);

            return Optional.of(factory);
        }

        private String formatFlinkPrefix(Factory formatFactory, ConfigOption<String> formatOption) {
            String identifier = formatFactory.factoryIdentifier();
            return getFormatPrefix(formatOption, identifier);
        }

        @SuppressWarnings({"unchecked"})
        private ReadableConfig createFormatOptions(
                String formatPrefix, FormatFactory formatFactory) {
            Set<ConfigOption<?>> forwardableConfigOptions = formatFactory.forwardOptions();
            Configuration formatConf = new DelegatingConfiguration(allOptions, formatPrefix);
            if (forwardableConfigOptions.isEmpty()) {
                return formatConf;
            }

            Configuration formatConfFromEnrichingOptions =
                    new DelegatingConfiguration(enrichingOptions, formatPrefix);

            for (ConfigOption<?> option : forwardableConfigOptions) {
                formatConfFromEnrichingOptions
                        .getOptional(option)
                        .ifPresent(o -> formatConf.set((ConfigOption<? super Object>) option, o));
            }

            return formatConf;
        }

        /**
         * This function assumes that the format config is used only and only if the original
         * configuration contains the format config option. It will fail if there is a mismatch of
         * the identifier between the format in the plan table map and the one in enriching table
         * map.
         */
        private void checkFormatIdentifierMatchesWithEnrichingOptions(
                ConfigOption<String> formatOption, String identifierFromPlan) {
            Optional<String> identifierFromEnrichingOptions =
                    enrichingOptions.getOptional(formatOption);

            if (!identifierFromEnrichingOptions.isPresent()) {
                return;
            }

            if (identifierFromPlan == null) {
                throw new ValidationException(
                        String.format(
                                "The persisted plan has no format option '%s' specified, while the catalog table has it with value '%s'. "
                                        + "This is invalid, as either only the persisted plan table defines the format, "
                                        + "or both the persisted plan table and the catalog table defines the same format.",
                                formatOption, identifierFromEnrichingOptions.get()));
            }

            if (!Objects.equals(identifierFromPlan, identifierFromEnrichingOptions.get())) {
                throw new ValidationException(
                        String.format(
                                "Both persisted plan table and catalog table define the format option '%s', "
                                        + "but they mismatch: '%s' != '%s'.",
                                formatOption,
                                identifierFromPlan,
                                identifierFromEnrichingOptions.get()));
            }
        }
    }
    // --------------------------------------------------------------------------------------------

    private FlinkFactoryUtil() {
        // no instantiation
    }
}
