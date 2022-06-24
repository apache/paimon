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
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.file.catalog.Catalog;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/** Factory for {@link FlinkCatalog}. */
public class FlinkCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "table-store";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key("default-database").stringType().defaultValue("default");

    public static final ConfigOption<String> WAREHOUSE =
            ConfigOptions.key("warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The warehouse root path of catalog.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(WAREHOUSE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(PROPERTY_VERSION);
        return options;
    }

    @Override
    public FlinkCatalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        return createCatalog(
                new Path(options.get(WAREHOUSE)), context.getName(), options.get(DEFAULT_DATABASE));
    }

    public static FlinkCatalog createCatalog(Path warehouse, String catalogName) {
        return createCatalog(warehouse, catalogName, DEFAULT_DATABASE.defaultValue());
    }

    public static FlinkCatalog createCatalog(
            Path warehouse, String catalogName, String defaultDatabase) {
        return new FlinkCatalog(Catalog.create(warehouse), catalogName, defaultDatabase);
    }
}
