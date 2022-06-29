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

package org.apache.flink.table.store.connector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.file.catalog.CatalogFactory;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/** Factory for {@link FlinkCatalog}. */
public class FlinkCatalogFactory implements org.apache.flink.table.factories.CatalogFactory {

    public static final String IDENTIFIER = "table-store";

    public static final ConfigOption<String> CATALOG_TYPE =
            ConfigOptions.key("catalog-type").stringType().noDefaultValue();
    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key("default-database").stringType().defaultValue("default");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public FlinkCatalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();
        return createCatalog(context.getName(), options);
    }

    public static FlinkCatalog createCatalog(String catalogName, ReadableConfig options) {
        // manual validation
        // because different catalog types may have different options
        // we can't list them all in the optionalOptions() method
        String catalogType =
                Preconditions.checkNotNull(
                        options.get(CATALOG_TYPE), "Table store catalog type must be set");

        List<CatalogFactory> factories = new ArrayList<>();
        ServiceLoader.load(CatalogFactory.class, Thread.currentThread().getContextClassLoader())
                .iterator()
                .forEachRemaining(
                        f -> {
                            if (f.identifier().equals(catalogType)) {
                                factories.add(f);
                            }
                        });
        if (factories.size() != 1) {
            throw new RuntimeException(
                    "Found "
                            + factories.size()
                            + " classes implementing "
                            + CatalogFactory.class.getName()
                            + " with type "
                            + catalogType
                            + ". They are:\n"
                            + factories.stream()
                                    .map(t -> t.getClass().getName())
                                    .collect(Collectors.joining("\n")));
        }

        return new FlinkCatalog(
                factories.get(0).create(options), catalogName, options.get(DEFAULT_DATABASE));
    }
}
