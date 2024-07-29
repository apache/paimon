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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.FileBasedPrivilegeManager;
import org.apache.paimon.privilege.PrivilegeManager;
import org.apache.paimon.privilege.PrivilegedCatalog;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.apache.paimon.options.CatalogOptions.CACHE_EXPIRATION_INTERVAL_MS;
import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

/**
 * Factory to create {@link Catalog}. Each factory should have a unique identifier.
 *
 * @since 0.4.0
 */
@Public
public interface CatalogFactory extends Factory {

    default Catalog create(FileIO fileIO, Path warehouse, CatalogContext context) {
        throw new UnsupportedOperationException(
                "Use  create(context) for " + this.getClass().getSimpleName());
    }

    default Catalog create(CatalogContext context) {
        throw new UnsupportedOperationException(
                "Use create(fileIO, warehouse, context) for " + this.getClass().getSimpleName());
    }

    static Path warehouse(CatalogContext context) {
        String warehouse =
                Preconditions.checkNotNull(
                        context.options().get(WAREHOUSE),
                        "Paimon '" + WAREHOUSE.key() + "' path must be set");
        return new Path(warehouse);
    }

    /**
     * If the ClassLoader is not specified, using the context ClassLoader of current thread as
     * default.
     */
    static Catalog createCatalog(CatalogContext options) {
        return createCatalog(options, CatalogFactory.class.getClassLoader());
    }

    static Catalog createCatalog(CatalogContext context, ClassLoader classLoader) {
        Catalog catalog = createUnwrappedCatalog(context, classLoader);

        Options options = context.options();
        if (options.get(CACHE_ENABLED)) {
            catalog = new CachingCatalog(catalog, options.get(CACHE_EXPIRATION_INTERVAL_MS));
        }

        PrivilegeManager privilegeManager =
                new FileBasedPrivilegeManager(
                        catalog.warehouse(),
                        catalog.fileIO(),
                        context.options().get(PrivilegedCatalog.USER),
                        context.options().get(PrivilegedCatalog.PASSWORD));
        if (privilegeManager.privilegeEnabled()) {
            catalog = new PrivilegedCatalog(catalog, privilegeManager);
        }

        return catalog;
    }

    static Catalog createUnwrappedCatalog(CatalogContext context, ClassLoader classLoader) {
        Options options = context.options();
        String metastore = options.get(METASTORE);
        CatalogFactory catalogFactory =
                FactoryUtil.discoverFactory(classLoader, CatalogFactory.class, metastore);

        try {
            return catalogFactory.create(context);
        } catch (UnsupportedOperationException ignore) {
        }

        // manual validation
        // because different catalog types may have different options
        // we can't list them all in the optionalOptions() method
        String warehouse = warehouse(context).toUri().toString();

        Path warehousePath = new Path(warehouse);
        FileIO fileIO;

        try {
            fileIO = FileIO.get(warehousePath, context);
            fileIO.checkOrMkdirs(warehousePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return catalogFactory.create(fileIO, warehousePath, context);
    }
}
