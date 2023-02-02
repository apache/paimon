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
import org.apache.flink.table.store.file.catalog.Catalog;
import org.apache.flink.table.store.file.catalog.CatalogFactory;
import org.apache.flink.table.store.options.CatalogOptions;

import java.util.Collections;
import java.util.Set;

/** Factory for {@link FlinkCatalog}. */
public class FlinkCatalogFactory implements org.apache.flink.table.factories.CatalogFactory {

    public static final String IDENTIFIER = "table-store";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key("default-database")
                    .stringType()
                    .defaultValue(Catalog.DEFAULT_DATABASE);

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
        return createCatalog(
                context.getName(),
                FlinkUtils.catalogOptions(context.getOptions(), context.getConfiguration()),
                context.getClassLoader());
    }

    public static FlinkCatalog createCatalog(
            String catalogName, CatalogOptions options, ClassLoader classLoader) {
        return new FlinkCatalog(
                CatalogFactory.createCatalog(options, classLoader),
                catalogName,
                options.get(DEFAULT_DATABASE));
    }
}
