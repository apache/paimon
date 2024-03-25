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

import org.apache.paimon.client.ClientPool;
import org.apache.paimon.jdbc.JdbcCatalogFactory;
import org.apache.paimon.jdbc.JdbcCatalogLock;
import org.apache.paimon.jdbc.JdbcClientPool;
import org.apache.paimon.jdbc.JdbcUtils;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Optional;

/** Utils for {@link org.apache.paimon.catalog.CatalogLock.LockContext}. */
public class LockContextUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemCatalog.class);

    public static Optional<CatalogLock.LockContext> lockContext(
            ClientPool.ClientPoolImpl clientPool, Options catalogOptions, String catalogKey) {
        if (clientPool == null) {
            return Optional.of(new AbstractCatalog.OptionLockContext(catalogOptions));
        }
        String lockType = catalogOptions.get(CatalogOptions.LOCK_TYPE);
        switch (lockType) {
            case JdbcCatalogFactory.IDENTIFIER:
                JdbcClientPool connections = (JdbcClientPool) clientPool;
                return Optional.of(
                        new JdbcCatalogLock.JdbcLockContext(
                                connections, catalogKey, catalogOptions));
            default:
                LOG.warn("Unsupported lock type:" + lockType);
                return Optional.of(new AbstractCatalog.OptionLockContext(catalogOptions));
        }
    }

    public static ClientPool.ClientPoolImpl tryInitializeClientPool(Options catalogOptions) {
        String lockType = catalogOptions.get(CatalogOptions.LOCK_TYPE);
        if (lockType == null) {
            return null;
        }
        switch (lockType) {
            case JdbcCatalogFactory.IDENTIFIER:
                JdbcClientPool connections =
                        new JdbcClientPool(
                                catalogOptions.get(CatalogOptions.CLIENT_POOL_SIZE),
                                catalogOptions.get(CatalogOptions.URI.key()),
                                catalogOptions.toMap());
                try {
                    JdbcUtils.createDistributedLockTable(connections, catalogOptions);
                } catch (SQLException e) {
                    throw new RuntimeException("Cannot initialize JDBC distributed lock.", e);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted in call to initialize", e);
                }
                return connections;
            default:
                LOG.warn("Unsupported lock type:" + lockType);
                return null;
        }
    }

    public static void close(ClientPool.ClientPoolImpl clientPool) {
        if (clientPool == null) {
            return;
        }
        if (clientPool instanceof JdbcClientPool) {
            JdbcClientPool connections = (JdbcClientPool) clientPool;
            if (!connections.isClosed()) {
                connections.close();
            }
        } else {
            clientPool.close();
        }
        clientPool = null;
    }
}
