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

package org.apache.paimon.client;

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkState;

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Client pool for using multiple clients to execute actions. */
public interface ClientPool<C, E extends Exception> {
    /** Action interface for client. */
    interface Action<R, C, E extends Exception> {
        R run(C client) throws E;
    }

    <R> R run(Action<R, C, E> action) throws E, InterruptedException;

    <R> R run(Action<R, C, E> action, boolean retry) throws E, InterruptedException;

    /** Default implementation for {@link ClientPool}. */
    abstract class ClientPoolImpl<C, E extends Exception>
            implements Closeable, Serializable, ClientPool<C, E> {
        private static final Logger LOG = LoggerFactory.getLogger(ClientPoolImpl.class);

        private final int poolSize;
        private final Deque<C> clients;
        private final Class<? extends E> reconnectExc;
        private final Object signal = new Object();
        private final boolean retryByDefault;
        private volatile int currentSize;
        private boolean closed;

        public ClientPoolImpl(
                int poolSize, Class<? extends E> reconnectExc, boolean retryByDefault) {
            this.poolSize = poolSize;
            this.reconnectExc = reconnectExc;
            this.clients = new ArrayDeque<>(poolSize);
            this.currentSize = 0;
            this.closed = false;
            this.retryByDefault = retryByDefault;
        }

        @Override
        public <R> R run(Action<R, C, E> action) throws E, InterruptedException {
            return run(action, retryByDefault);
        }

        @Override
        public <R> R run(Action<R, C, E> action, boolean retry) throws E, InterruptedException {
            C client = get();
            try {
                return action.run(client);
            } catch (Exception exc) {
                if (retry && isConnectionException(exc)) {
                    try {
                        client = reconnect(client);
                    } catch (Exception ignored) {
                        // if reconnection throws any exception, rethrow the original failure
                        throw reconnectExc.cast(exc);
                    }

                    return action.run(client);
                }

                throw exc;

            } finally {
                release(client);
            }
        }

        protected abstract C newClient();

        protected abstract C reconnect(C client);

        protected boolean isConnectionException(Exception exc) {
            return reconnectExc.isInstance(exc);
        }

        protected abstract void close(C client);

        @Override
        public void close() {
            this.closed = true;
            try {
                while (currentSize > 0) {
                    if (!clients.isEmpty()) {
                        synchronized (this) {
                            if (!clients.isEmpty()) {
                                C client = clients.removeFirst();
                                close(client);
                                currentSize -= 1;
                            }
                        }
                    }
                    if (clients.isEmpty() && currentSize > 0) {
                        synchronized (signal) {
                            // wake every second in case this missed the signal
                            signal.wait(1000);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn(
                        "Interrupted while shutting down pool. Some clients may not be closed.", e);
            }
        }

        private C get() throws InterruptedException {
            checkState(!closed, "Cannot get a client from a closed pool");
            while (true) {
                if (!clients.isEmpty() || currentSize < poolSize) {
                    synchronized (this) {
                        if (!clients.isEmpty()) {
                            return clients.removeFirst();
                        } else if (currentSize < poolSize) {
                            C client = newClient();
                            currentSize += 1;
                            return client;
                        }
                    }
                }
                synchronized (signal) {
                    // wake every second in case this missed the signal
                    signal.wait(1000);
                }
            }
        }

        private void release(C client) {
            synchronized (this) {
                clients.addFirst(client);
            }
            synchronized (signal) {
                signal.notify();
            }
        }

        public int poolSize() {
            return poolSize;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    /** Cached client pool for {@link ClientPool}. */
    abstract class CachedClientPool<C, E extends Exception, CP extends ClientPoolImpl>
            implements Closeable, Serializable, ClientPool<C, E> {

        protected static final String CONF_KEY_PREFIX = "confKey:";
        protected final long evictionInterval;
        protected final String key;
        protected final String metadata;
        private final Options options;

        public CachedClientPool(Options options) {
            this.options = options;
            this.evictionInterval =
                    options.get(CatalogOptions.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS);
            this.metadata = options.get(CatalogOptions.METASTORE);
            this.key = extractKey(options);
            init();
        }

        protected Options options() {
            return options;
        }

        protected abstract void init();

        protected abstract ClientPool<C, E> clientPool();

        @Override
        public <R> R run(Action<R, C, E> action) throws E, InterruptedException {
            return clientPool().run(action);
        }

        @Override
        public <R> R run(Action<R, C, E> action, boolean retry) throws E, InterruptedException {
            return clientPool().run(action, retry);
        }

        private String extractKey(Options options) {
            List<Object> elements = Lists.newArrayList();
            elements.add(options.get(CatalogOptions.URI));
            String metastore = options.get(CatalogOptions.METASTORE);
            elements.add(metastore);
            String catalogKey = options.getOptional(CatalogOptions.CATALOG_KEY).orElse(metastore);
            elements.add(catalogKey);
            elements.addAll(extractKeyElement());
            return CONF_KEY_PREFIX.concat(StringUtils.join(elements, "."));
        }

        protected abstract List<String> extractKeyElement();

        @Override
        public void close() throws IOException {
            // Do nothing, will automatically clean up
        }
    }
}
