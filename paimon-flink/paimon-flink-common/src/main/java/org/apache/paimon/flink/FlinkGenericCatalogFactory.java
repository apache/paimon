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

package org.apache.paimon.flink;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.hive.HiveCatalogOptions;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.flink.FlinkCatalogOptions.DEFAULT_DATABASE;

/** Factory for {@link FlinkGenericCatalog}. */
public class FlinkGenericCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "paimon-generic";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<org.apache.flink.configuration.ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<org.apache.flink.configuration.ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public FlinkGenericCatalog createCatalog(Context context) {
        CatalogFactory hiveFactory = createHiveCatalogFactory(context.getClassLoader());
        Context filteredContext = filterContextOptions(context, hiveFactory);
        Catalog catalog = hiveFactory.createCatalog(filteredContext);
        return createCatalog(
                context.getClassLoader(), context.getOptions(), context.getName(), catalog);
    }

    @VisibleForTesting
    public Context filterContextOptions(Context context, CatalogFactory catalogFactory) {
        Set<ConfigOption<?>> catalogOptions = new HashSet<>(catalogFactory.requiredOptions());
        catalogOptions.addAll(catalogFactory.optionalOptions());
        Map<String, String> contextOptions = context.getOptions();
        Map<String, String> flinkCatalogOptions = new HashMap<>();
        catalogOptions.forEach(
                option -> {
                    if (contextOptions.containsKey(option.key())) {
                        flinkCatalogOptions.put(option.key(), contextOptions.get(option.key()));
                    }
                });
        return new FactoryUtil.DefaultCatalogContext(
                context.getName(),
                flinkCatalogOptions,
                context.getConfiguration(),
                context.getClassLoader());
    }

    @VisibleForTesting
    public static FlinkGenericCatalog createCatalog(
            ClassLoader cl, Map<String, String> optionMap, String name, Catalog flinkCatalog) {
        Options options = Options.fromMap(optionMap);
        options.set(CatalogOptions.METASTORE, "hive");
        options.set(HiveCatalogOptions.FORMAT_TABLE_ENABLED, false);
        FlinkCatalog paimon =
                new FlinkCatalog(
                        org.apache.paimon.catalog.CatalogFactory.createCatalog(
                                CatalogContext.create(options, new FlinkFileIOLoader()), cl),
                        name,
                        options.get(DEFAULT_DATABASE),
                        cl,
                        options);

        return new FlinkGenericCatalog(paimon, flinkCatalog);
    }

    private static CatalogFactory createHiveCatalogFactory(ClassLoader cl) {
        return FactoryUtil.discoverFactory(cl, CatalogFactory.class, "hive");
    }
}
