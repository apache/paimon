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

package org.apache.paimon.flink.pipeline.cdc;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.FallbackKey;

import org.apache.flink.util.IterableUtils;

import java.time.Duration;

/** Options for the cdc source. */
public class CDCOptions {
    /** prefix for passing properties for catalog creation. */
    public static final String PREFIX_CATALOG_PROPERTIES = "catalog.properties.";

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database to be scanned. By default, all databases will be scanned.");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the table to be scanned. By default, all tables will be scanned.");

    public static final ConfigOption<Duration> TABLE_DISCOVERY_INTERVAL =
            ConfigOptions.key("table.discovery-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The discovery interval of new tables. Only effective when database or table is not set.");

    public static <T> org.apache.flink.cdc.common.configuration.ConfigOption<T> toCDCOption(
            org.apache.paimon.options.ConfigOption<T> option) {
        org.apache.flink.cdc.common.configuration.ConfigOptions.OptionBuilder builder =
                org.apache.flink.cdc.common.configuration.ConfigOptions.key(option.key());
        org.apache.flink.cdc.common.configuration.ConfigOption<T> cdcOption =
                configTypeAndDefaultValue(builder, option.defaultValue());

        return cdcOption
                .withDescription(option.description().toString())
                .withFallbackKeys(
                        IterableUtils.toStream(option.fallbackKeys())
                                .map(FallbackKey::getKey)
                                .toArray(String[]::new))
                .withDeprecatedKeys(
                        IterableUtils.toStream(option.deprecatedKeys()).toArray(String[]::new));
    }

    @SuppressWarnings("unchecked")
    private static <T>
            org.apache.flink.cdc.common.configuration.ConfigOption<T> configTypeAndDefaultValue(
                    org.apache.flink.cdc.common.configuration.ConfigOptions.OptionBuilder builder,
                    T defaultValue) {
        if (defaultValue instanceof String) {
            return (org.apache.flink.cdc.common.configuration.ConfigOption<T>)
                    builder.stringType().defaultValue((String) defaultValue);
        } else if (defaultValue instanceof Integer) {
            return (org.apache.flink.cdc.common.configuration.ConfigOption<T>)
                    builder.intType().defaultValue((Integer) defaultValue);
        } else if (defaultValue instanceof Long) {
            return (org.apache.flink.cdc.common.configuration.ConfigOption<T>)
                    builder.longType().defaultValue((Long) defaultValue);
        } else if (defaultValue instanceof Boolean) {
            return (org.apache.flink.cdc.common.configuration.ConfigOption<T>)
                    builder.booleanType().defaultValue((Boolean) defaultValue);
        } else if (defaultValue instanceof Double) {
            return (org.apache.flink.cdc.common.configuration.ConfigOption<T>)
                    builder.doubleType().defaultValue((Double) defaultValue);
        } else if (defaultValue instanceof Float) {
            return (org.apache.flink.cdc.common.configuration.ConfigOption<T>)
                    builder.floatType().defaultValue((Float) defaultValue);
        } else if (defaultValue instanceof Duration) {
            return (org.apache.flink.cdc.common.configuration.ConfigOption<T>)
                    builder.durationType().defaultValue((Duration) defaultValue);
        } else {
            // Default to string type for other types
            return (org.apache.flink.cdc.common.configuration.ConfigOption<T>)
                    builder.stringType()
                            .defaultValue(defaultValue != null ? defaultValue.toString() : null);
        }
    }
}
