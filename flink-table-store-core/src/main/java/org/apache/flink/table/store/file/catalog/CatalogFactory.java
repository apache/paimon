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

package org.apache.flink.table.store.file.catalog;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.CatalogOptions.METASTORE;
import static org.apache.flink.table.store.CatalogOptions.WAREHOUSE;

/** Factory to create {@link Catalog}. Each factory should have a unique identifier. */
public interface CatalogFactory {

    String identifier();

    Catalog create(String warehouse, Configuration options);

    static Catalog createCatalog(Configuration options) {
        // manual validation
        // because different catalog types may have different options
        // we can't list them all in the optionalOptions() method
        String warehouse =
                Preconditions.checkNotNull(
                        options.get(WAREHOUSE),
                        "Table store '" + WAREHOUSE.key() + "' path must be set");

        String metastore = options.get(METASTORE);
        List<CatalogFactory> factories = new ArrayList<>();
        ServiceLoader.load(CatalogFactory.class, Thread.currentThread().getContextClassLoader())
                .iterator()
                .forEachRemaining(
                        f -> {
                            if (f.identifier().equals(metastore)) {
                                factories.add(f);
                            }
                        });
        if (factories.size() != 1) {
            throw new RuntimeException(
                    "Found "
                            + factories.size()
                            + " classes implementing "
                            + CatalogFactory.class.getName()
                            + " with metastore "
                            + metastore
                            + ". They are:\n"
                            + factories.stream()
                                    .map(t -> t.getClass().getName())
                                    .collect(Collectors.joining("\n")));
        }

        return factories.get(0).create(warehouse, options);
    }
}
