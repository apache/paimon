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

package org.apache.paimon.hive.pool;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.hive.HiveCatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Scheduler;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.guava30.com.google.common.collect.Sets;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.hive.HiveCatalogOptions.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS;
import static org.apache.paimon.hive.HiveCatalogOptions.CLIENT_POOL_CACHE_KEYS;
import static org.apache.paimon.options.CatalogOptions.CLIENT_POOL_SIZE;

/**
 * A ClientPool that caches the underlying HiveClientPool instances.
 *
 * <p>Mostly copied from iceberg.
 */
public class CachedClientPool implements ClientPool<IMetaStoreClient, TException> {

    private static final String CONF_ELEMENT_PREFIX = "conf:";

    private static Cache<Key, HiveClientPool> clientPoolCache;

    private final Configuration conf;
    private final int clientPoolSize;
    private final long evictionInterval;
    private final Key key;
    private final String clientClassName;

    public CachedClientPool(Configuration conf, Options options, String clientClassName) {
        this.conf = conf;
        this.clientPoolSize = options.get(CLIENT_POOL_SIZE);
        this.evictionInterval = options.get(CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS);
        this.key = extractKey(clientClassName, options.get(CLIENT_POOL_CACHE_KEYS), conf);
        this.clientClassName = clientClassName;
        init();
        // set ugi information to hms client
        try {
            run(client -> null);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    HiveClientPool clientPool() {
        return clientPoolCache.get(
                key, k -> new HiveClientPool(clientPoolSize, conf, clientClassName));
    }

    private synchronized void init() {
        if (clientPoolCache == null) {
            // Since Caffeine does not ensure that removalListener will be involved after expiration
            // We use a scheduler with one thread to clean up expired clients.
            clientPoolCache =
                    Caffeine.newBuilder()
                            .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                            .removalListener(
                                    (ignored, value, cause) -> {
                                        if (value != null) {
                                            ((HiveClientPool) value).close();
                                        }
                                    })
                            .scheduler(
                                    Scheduler.forScheduledExecutorService(
                                            Executors.newSingleThreadScheduledExecutor(
                                                    new ThreadFactoryBuilder()
                                                            .setDaemon(true)
                                                            .setNameFormat("hive-metastore-cleaner")
                                                            .build())))
                            .build();
        }
    }

    @VisibleForTesting
    static Cache<Key, HiveClientPool> clientPoolCache() {
        return clientPoolCache;
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action)
            throws TException, InterruptedException {
        return clientPool().run(action);
    }

    @Override
    public void execute(ExecuteAction<IMetaStoreClient, TException> action)
            throws TException, InterruptedException {
        clientPool().execute(action);
    }

    @VisibleForTesting
    static Key extractKey(String clientClassName, String cacheKeys, Configuration conf) {
        // generate key elements in a certain order, so that the Key instances are comparable
        List<Object> elements = Lists.newArrayList();
        elements.add(clientClassName);
        elements.add(conf.get(HiveConf.ConfVars.METASTOREURIS.varname, ""));
        elements.add(HiveCatalogOptions.IDENTIFIER);
        if (cacheKeys == null || cacheKeys.isEmpty()) {
            return Key.of(elements);
        }

        Set<KeyElementType> types = Sets.newTreeSet(Comparator.comparingInt(Enum::ordinal));
        Map<String, String> confElements = Maps.newTreeMap();
        for (String element : cacheKeys.split(",", -1)) {
            String trimmed = element.trim();
            if (trimmed.toLowerCase(Locale.ROOT).startsWith(CONF_ELEMENT_PREFIX)) {
                String key = trimmed.substring(CONF_ELEMENT_PREFIX.length());

                Preconditions.checkArgument(
                        !confElements.containsKey(key),
                        "Conf key element %s already specified",
                        key);
                confElements.put(key, conf.get(key));
            } else {
                KeyElementType type = KeyElementType.valueOf(trimmed.toUpperCase());
                switch (type) {
                    case UGI:
                    case USER_NAME:
                        Preconditions.checkArgument(
                                !types.contains(type),
                                "%s key element already specified",
                                type.name());
                        types.add(type);
                        break;
                    default:
                        throw new RuntimeException("Unknown key element %s" + trimmed);
                }
            }
        }
        for (KeyElementType type : types) {
            switch (type) {
                case UGI:
                    try {
                        elements.add(UserGroupInformation.getCurrentUser());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    break;
                case USER_NAME:
                    try {
                        elements.add(UserGroupInformation.getCurrentUser().getUserName());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    break;
                default:
                    throw new RuntimeException("Unexpected key element " + type.name());
            }
        }
        for (String key : confElements.keySet()) {
            elements.add(ConfElement.of(key, confElements.get(key)));
        }
        return Key.of(elements);
    }

    @Override
    public void close() throws IOException {
        if (clientPoolCache != null) {
            clientPoolCache.asMap().values().forEach(HiveClientPool::close);
            clientPoolCache.cleanUp();
        }
    }

    static class Key {
        private final List<Object> elements;

        private Key(List<Object> elements) {
            this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
        }

        public List<Object> elements() {
            return elements;
        }

        public static Key of(List<Object> elements) {
            return new Key(elements);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Key that = (Key) o;
            if (this.elements.size() != that.elements.size()) {
                return false;
            }
            for (int i = 0; i < elements.size(); i++) {
                if (!Objects.equals(this.elements.get(i), that.elements.get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            synchronized (elements) {
                for (Object p : elements) {
                    hashCode ^= p.hashCode();
                }
            }
            return hashCode;
        }
    }

    static class ConfElement {
        private final String key;
        private final String value;

        private ConfElement(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        @Nullable
        public String value() {
            return value;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ConfElement other = (ConfElement) obj;
            return Objects.equals(key, other.key) && Objects.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        public static ConfElement of(String key, String value) {
            return new ConfElement(key, value);
        }
    }

    private enum KeyElementType {
        UGI,
        USER_NAME,
        CONF
    }
}
