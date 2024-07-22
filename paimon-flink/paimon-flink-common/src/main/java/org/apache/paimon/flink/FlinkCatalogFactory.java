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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.util.Collections;
import java.util.Set;

import static org.apache.paimon.flink.FlinkCatalogOptions.DEFAULT_DATABASE;

/** Factory for {@link FlinkCatalog}. */
public class FlinkCatalogFactory implements org.apache.flink.table.factories.CatalogFactory {

    public static final String IDENTIFIER = "paimon";

    public static final FlinkCatalogFactory INSTANCE = new FlinkCatalogFactory();

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
    public FlinkCatalog createCatalog(Context context) {
        return createCatalog(
                context.getName(),
                CatalogContext.create(
                        Options.fromMap(context.getOptions()), new FlinkFileIOLoader()),
                context.getClassLoader());
    }

    public FlinkCatalog createCatalog(
            String catalogName, CatalogContext context, ClassLoader classLoader) {
        return new FlinkCatalog(
                CatalogFactory.createCatalog(context, classLoader),
                catalogName,
                context.options().get(DEFAULT_DATABASE),
                classLoader,
                context.options());
    }

    public FlinkCatalog createCatalog(String catalogName, Catalog catalog, Options options) {
        return new FlinkCatalog(
                catalog,
                catalogName,
                Catalog.DEFAULT_DATABASE,
                FlinkCatalogFactory.class.getClassLoader(),
                options);
    }

    public Catalog createPaimonCatalog(Options catalogOptions) {
        return CatalogFactory.createCatalog(
                CatalogContext.create(catalogOptions, new FlinkFileIOLoader()));
    }
}
