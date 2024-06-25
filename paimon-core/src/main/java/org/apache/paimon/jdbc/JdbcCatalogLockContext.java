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

package org.apache.paimon.jdbc;

import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

/** Jdbc lock context. */
public class JdbcCatalogLockContext implements CatalogLockContext {

    private transient JdbcClientPool connections;
    private final String catalogKey;
    private final Options options;

    public JdbcCatalogLockContext(String catalogKey, Options options) {
        this.catalogKey = catalogKey;
        this.options = options;
    }

    @Override
    public Options options() {
        return options;
    }

    public JdbcClientPool connections() {
        if (connections == null) {
            connections =
                    new JdbcClientPool(
                            options.get(CatalogOptions.CLIENT_POOL_SIZE),
                            options.get(CatalogOptions.URI.key()),
                            options.toMap());
        }
        return connections;
    }

    public String catalogKey() {
        return catalogKey;
    }
}
