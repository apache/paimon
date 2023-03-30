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

    /**
     * Describes the property version. This can be used for backwards compatibility in case the
     * property format changes.
     */
    public static final ConfigOption<Integer> PROPERTY_VERSION =
            ConfigOptions.key("property-version")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Version of the overall property design. This option is meant for future backwards compatibility.");

    public static final ConfigOption<String> CONNECTOR =
            ConfigOptions.key("connector")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Uniquely identifies the connector of a dynamic table that is used for accessing data in "
                                    + "an external system. Its value is used during table source and table sink discovery.");

    public static final ConfigOption<String> FORMAT =
            ConfigOptions.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom parallelism for the sink. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    public static final ConfigOption<List<String>> SQL_GATEWAY_ENDPOINT_TYPE =
            ConfigOptions.key("sql-gateway.endpoint.type")
                    .stringType()
                    .asList()
                    .defaultValues("rest")
                    .withDescription("Specify the endpoints that are used.");

    /**
     * Suffix for keys of {@link ConfigOption} in case a connector requires multiple formats (e.g.
     * for both key and value).
     *
     * <p>See {@link #createFlinkTableFactoryHelper(DynamicTablePaimonFactory, Context)} Context)}
     * for more information.
     */
    public static final String FORMAT_SUFFIX = ".format";

    /**
     * The placeholder symbol to be used for keys of options which can be templated. See {@link
     * PaimonFactory} for details.
     */
    public static final String PLACEHOLDER_SYMBOL = "#";

    /**
     * Creates a utility that helps in discovering formats, merging options with {@link
     * org.apache.flink.table.factories.DynamicTableFactory.Context#getEnrichmentOptions()} and
     * validating them all for a {@link DynamicTablePaimonFactory}.
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
     *
     * <p>Note: The format option parameter of {@link
     * FlinkTableFactoryHelper#discoverEncodingFormat(Class, ConfigOption)} and {@link
     * FlinkTableFactoryHelper#discoverDecodingFormat(Class, ConfigOption)} must be {@link #FORMAT}
     * or end with {@link #FORMAT_SUFFIX}. The discovery logic will replace 'format' with the
     * factory identifier value as the format prefix. For example, assuming the identifier is
     * 'json', if the format option key is 'format', then the format prefix is 'json.'. If the
     * format option key is 'value.format', then the format prefix is 'value.json'. The format
     * prefix is used to project the options for the format factory.
     *
     * <p>Note: When created, this utility merges the options from {@link
     * org.apache.flink.table.factories.DynamicTableFactory.Context#getEnrichmentOptions()} using
     * {@link DynamicTablePaimonFactory#forwardOptions()}.
     */
    public static FlinkTableFactoryHelper createFlinkTableFactoryHelper(
            DynamicTablePaimonFactory factory,
            org.apache.flink.table.factories.DynamicTableFactory.Context context) {
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

    /**
     * Validates the required and optional {@link ConfigOption}s of a paimonFactory.
     *
     * <p>Note: It does not check for left-over options.
     */
    public static void validateFactoryOptions(PaimonFactory paimonFactory, ReadableConfig options) {
        validateFactoryOptions(
                paimonFactory.requiredOptions(), paimonFactory.optionalOptions(), options);
    }

    /**
     * Validates the required options and optional options.
     *
     * <p>Note: It does not check for left-over options.
     */
    public static void validateFactoryOptions(
            Set<ConfigOption<?>> requiredOptions,
            Set<ConfigOption<?>> optionalOptions,
            ReadableConfig options) {
        // currently Flink's options have no validation feature which is why we access them eagerly
        // to provoke a parsing error

        final List<String> missingRequiredOptions =
                requiredOptions.stream()
                        // Templated options will never appear with their template key, so we need
                        // to ignore them as required properties here
                        .filter(
                                option ->
                                        allKeys(option)
                                                .noneMatch(k -> k.contains(PLACEHOLDER_SYMBOL)))
                        .filter(option -> readOption(options, option) == null)
                        .map(ConfigOption::key)
                        .sorted()
                        .collect(Collectors.toList());

        if (!missingRequiredOptions.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "One or more required options are missing.\n\n"
                                    + "Missing required options are:\n\n"
                                    + "%s",
                            String.join("\n", missingRequiredOptions)));
        }

        optionalOptions.forEach(option -> readOption(options, option));
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
                                    + PaimonFactory.class.getCanonicalName()
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

    private static <T> T readOption(ReadableConfig options, ConfigOption<T> option) {
        try {
            return options.get(option);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format("Invalid value for option '%s'.", option.key()), t);
        }
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

    /** Base flink helper utility for validating all options for a {@link PaimonFactory}. */
    public static class FlinkFactoryHelper<F extends PaimonFactory> {

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
            consumedOptions.addAll(factory.requiredOptions());
            consumedOptions.addAll(factory.optionalOptions());

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
     * @see #createFlinkTableFactoryHelper(DynamicTablePaimonFactory, Context)
     */
    public static class FlinkTableFactoryHelper
            extends FlinkFactoryHelper<DynamicTablePaimonFactory> {

        private final Context context;

        private final Configuration enrichingOptions;

        private FlinkTableFactoryHelper(DynamicTablePaimonFactory tableFactory, Context context) {
            super(
                    tableFactory,
                    context.getCatalogTable().getOptions(),
                    PROPERTY_VERSION,
                    CONNECTOR);
            this.context = context;
            this.enrichingOptions = Configuration.fromMap(context.getEnrichmentOptions());
            this.forwardOptions();
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

        /**
         * Forwards the options declared in {@link DynamicTableFactory#forwardOptions()} and
         * possibly {@link FormatFactory#forwardOptions()} from {@link
         * Context#getEnrichmentOptions()} to the final options, if present.
         */
        @SuppressWarnings({"unchecked"})
        private void forwardOptions() {
            for (ConfigOption<?> option : factory.forwardOptions()) {
                enrichingOptions
                        .getOptional(option)
                        .ifPresent(o -> allOptions.set((ConfigOption<? super Object>) option, o));
            }
        }

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
