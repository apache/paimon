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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.ReflectionUtils;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.configuration.ReadableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.DATABASE;
import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.PREFIX_CATALOG_PROPERTIES;
import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.TABLE;
import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.TABLE_DISCOVERY_INTERVAL;
import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.toCDCOption;

/** The {@link DataSourceFactory} for cdc source. */
public class CDCDataSourceFactory implements DataSourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CDCDataSourceFactory.class);

    public static final String IDENTIFIER = "paimon";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validateExcept(PREFIX_CATALOG_PROPERTIES);

        Map<String, String> catalogOptions = new HashMap<>();
        Map<String, String> cdcConfig = new HashMap<>();
        context.getFactoryConfiguration()
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(PREFIX_CATALOG_PROPERTIES)) {
                                catalogOptions.put(
                                        key.substring(PREFIX_CATALOG_PROPERTIES.length()), value);
                            } else {
                                cdcConfig.put(key, value);
                            }
                        });

        org.apache.flink.configuration.Configuration flinkConfig;
        boolean isReadableConfigAcquired = false;
        try {
            Method getFlinkConfMethod =
                    ReflectionUtils.getMethod(context.getClass(), "getFlinkConf", 0);
            ReadableConfig readableConfig = (ReadableConfig) getFlinkConfMethod.invoke(context);
            isReadableConfigAcquired = true;
            if (readableConfig instanceof org.apache.flink.configuration.Configuration) {
                flinkConfig = (org.apache.flink.configuration.Configuration) readableConfig;
            } else {
                Method toMapMethod =
                        ReflectionUtils.getMethod(readableConfig.getClass(), "toMap", 0);
                Map<String, String> configMap =
                        (Map<String, String>) toMapMethod.invoke(readableConfig);
                flinkConfig = org.apache.flink.configuration.Configuration.fromMap(configMap);
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            String failReason = isReadableConfigAcquired ? "Flink CDC 3.5-" : "Flink 1.18-";
            LOG.info(
                    "Cannot get Flink configuration from context. It is possibly due to compatibility with {}. Using empty Flink configuration to create catalog instead.",
                    failReason);
            flinkConfig = new org.apache.flink.configuration.Configuration();
        }

        return new CDCDataSource(
                CatalogContext.create(Options.fromMap(catalogOptions)),
                Configuration.fromMap(cdcConfig),
                flinkConfig);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(toCDCOption(DATABASE));
        set.add(toCDCOption(TABLE));
        set.add(toCDCOption(TABLE_DISCOVERY_INTERVAL));
        return set;
    }
}
