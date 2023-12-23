/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.paimon.hive.pool;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.hive.HiveClientPool;
import org.apache.paimon.options.Options;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;

import java.util.concurrent.TimeUnit;

import static org.apache.paimon.hive.HiveCatalogOptions.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS;
import static org.apache.paimon.hive.HiveCatalogOptions.CLIENT_POOL_SIZE;

/**
 * CachedClientPool.
 *
 * <p>Mostly copied from iceberg.
 */
public class CachedClientPool implements ClientPool<IMetaStoreClient, TException> {

    private static Cache<String, HiveClientPool> clientPoolCache;

    private final Configuration conf;
    private final String metastoreUri;
    private final int clientPoolSize;
    private final long evictionInterval;

    private final String clientClassName;

    public CachedClientPool(HiveConf conf, Options options, String clientClassName) {
        this.conf = conf;
        this.metastoreUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
        this.clientPoolSize = options.get(CLIENT_POOL_SIZE);
        this.evictionInterval = options.get(CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS);
        this.clientClassName = clientClassName;
        init();
    }

    public HiveClientPool clientPool() {
        return clientPoolCache.get(
                metastoreUri, k -> new HiveClientPool(clientPoolSize, conf, clientClassName));
    }

    private synchronized void init() {
        if (clientPoolCache == null) {
            clientPoolCache =
                    Caffeine.newBuilder()
                            .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                            .removalListener(
                                    (key, value, cause) -> ((HiveClientPool) value).close())
                            .build();
        }
    }

    @Override
    public <R> R run(RunAction<R, IMetaStoreClient, TException> action) throws TException {
        return clientPool().run(action);
    }

    @Override
    public <R> R run(RunAction<R, IMetaStoreClient, TException> action, boolean retry)
            throws TException {
        return clientPool().run(action, retry);
    }

    @Override
    public void execute(ExecuteAction<IMetaStoreClient, TException> action) throws TException {
        clientPool().execute(action);
    }

    @Override
    public void execute(ExecuteAction<IMetaStoreClient, TException> action, boolean retry)
            throws TException {
        clientPool().execute(action, retry);
    }

    @VisibleForTesting
    static Cache<String, HiveClientPool> clientPoolCache() {
        return clientPoolCache;
    }
}
