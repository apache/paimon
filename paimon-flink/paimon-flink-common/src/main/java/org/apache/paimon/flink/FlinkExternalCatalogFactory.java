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
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.CatalogFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.flink.FlinkCatalogOptions.DEFAULT_DATABASE;
import static org.apache.paimon.options.CatalogOptions.METASTORE;

/** Factory for {@link FlinkExternalCatalog}. */
public class FlinkExternalCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "paimon-external";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public FlinkExternalCatalog createCatalog(Context context) {
        return createCatalog(context.getClassLoader(), context.getOptions(), context.getName());
    }

    @VisibleForTesting
    public static FlinkExternalCatalog createCatalog(
            ClassLoader cl, Map<String, String> optionMap, String name) {
        Options options = Options.fromMap(optionMap);
        FlinkFileIOLoader fallbackIOLoader = new FlinkFileIOLoader();
        CatalogContext context = CatalogContext.create(options, fallbackIOLoader);
        String metastore = context.options().get(METASTORE);
        org.apache.paimon.catalog.CatalogFactory catalogFactory =
                FactoryUtil.discoverFactory(
                        cl, org.apache.paimon.catalog.CatalogFactory.class, metastore);
        String warehouse =
                org.apache.paimon.catalog.CatalogFactory.warehouse(context).toUri().toString();
        Path warehousePath = new Path(warehouse);
        try {
            FileIO fileIO = FileIO.get(warehousePath, context);
            FlinkCatalog paimon =
                    new FlinkCatalog(
                            catalogFactory.create(fileIO, warehousePath, context),
                            name,
                            options.get(DEFAULT_DATABASE),
                            cl,
                            options);
            return new FlinkExternalCatalog(paimon, fileIO, warehouse);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return CatalogFactory.super.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return CatalogFactory.super.optionalOptions();
    }
}
