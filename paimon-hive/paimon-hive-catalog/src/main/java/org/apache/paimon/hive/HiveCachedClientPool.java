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

package org.apache.paimon.hive;

import org.apache.paimon.client.ClientPool;
import org.apache.paimon.options.Options;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Scheduler;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Cache HiveClientPool, share connection pool between multiple tasks to prevent excessive
 * MetadataClient requests.
 */
public class HiveCachedClientPool
        extends ClientPool.CachedClientPool<IMetaStoreClient, TException, HiveClientPool> {

    protected static Cache<String, ClientPoolImpl> clientPoolCache;
    private final SerializableHiveConf hiveConf;
    private final String clientClassName;
    private final int poolSize;

    public HiveCachedClientPool(
            int poolSize, SerializableHiveConf hiveConf, String clientClassName, Options options) {
        super(options);
        this.poolSize = poolSize;
        this.hiveConf = hiveConf;
        this.clientClassName = clientClassName;
    }

    @Override
    protected synchronized void init() {
        if (clientPoolCache == null) {
            clientPoolCache =
                    Caffeine.newBuilder()
                            .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                            .removalListener(
                                    (ignored, value, cause) -> ((ClientPoolImpl) value).close())
                            .scheduler(
                                    Scheduler.forScheduledExecutorService(
                                            new ScheduledThreadPoolExecutor(
                                                    1,
                                                    new ThreadFactory() {
                                                        final ThreadFactory defaultFactory =
                                                                Executors.defaultThreadFactory();

                                                        @Override
                                                        public Thread newThread(Runnable r) {
                                                            Thread thread =
                                                                    defaultFactory.newThread(r);
                                                            thread.setDaemon(true);
                                                            return thread;
                                                        }
                                                    })))
                            .build();
        }
    }

    @Override
    public HiveClientPool clientPool() {
        return (HiveClientPool)
                clientPoolCache.get(
                        key, k -> new HiveClientPool(poolSize, hiveConf, clientClassName));
    }

    @Override
    protected List<String> extractKeyElement() {
        List<String> elements = Lists.newArrayList();
        elements.add(options().get(HiveCatalogFactory.METASTORE_CLIENT_CLASS));
        return elements;
    }
}
