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

package org.apache.paimon.flink.lakestream;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A fake lake-stream store {@link CatalogFactory} for tests. It is registered under the identifier
 * {@code fakestore} in the test-scope {@code
 * META-INF/services/org.apache.flink.table.factories.Factory} and plays the role that Fluss plays
 * in production, without any external dependency.
 */
public class FakeStoreCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "fakestore";

    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("bootstrap.servers").stringType().noDefaultValue();

    private static final List<Map<String, String>> CREATED_CATALOG_OPTIONS = new ArrayList<>();
    private static final List<ReadableConfig> CREATED_CATALOG_CONFIGURATIONS = new ArrayList<>();
    private static final List<FakeStoreCatalog> CREATED_CATALOGS = new ArrayList<>();

    public static List<Map<String, String>> createdCatalogOptions() {
        return CREATED_CATALOG_OPTIONS;
    }

    /** The Flink configurations the created store catalogs were handed, in creation order. */
    public static List<ReadableConfig> createdCatalogConfigurations() {
        return CREATED_CATALOG_CONFIGURATIONS;
    }

    public static List<FakeStoreCatalog> createdCatalogs() {
        return CREATED_CATALOGS;
    }

    public static void reset() {
        CREATED_CATALOG_OPTIONS.clear();
        CREATED_CATALOG_CONFIGURATIONS.clear();
        CREATED_CATALOGS.clear();
        FakeStoreCatalog.resetGetTableCalls();
        FakeStoreTableFactory.reset();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(BOOTSTRAP_SERVERS);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public Catalog createCatalog(Context context) {
        Map<String, String> options = new HashMap<>(context.getOptions());
        CREATED_CATALOG_OPTIONS.add(options);
        CREATED_CATALOG_CONFIGURATIONS.add(context.getConfiguration());
        if (!options.containsKey(BOOTSTRAP_SERVERS.key())) {
            throw new ValidationException(
                    String.format(
                            "Missing required option '%s' for the '%s' catalog.",
                            BOOTSTRAP_SERVERS.key(), IDENTIFIER));
        }
        FakeStoreCatalog catalog = new FakeStoreCatalog(context.getName(), options);
        CREATED_CATALOGS.add(catalog);
        return catalog;
    }
}
