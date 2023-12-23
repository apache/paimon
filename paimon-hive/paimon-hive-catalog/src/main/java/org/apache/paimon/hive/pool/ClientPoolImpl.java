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

import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * ClientPoolImpl.
 *
 * <p>Mostly copied from iceberg.
 */
public abstract class ClientPoolImpl<C, E extends Exception>
        implements Closeable, ClientPool<C, E> {
    private static final Logger LOG = LoggerFactory.getLogger(ClientPoolImpl.class);

    private final int poolSize;
    private final Deque<C> clients;
    private final Class<? extends E> reconnectExc;
    private final Object signal = new Object();
    private final boolean retryByDefault;
    private volatile int currentSize;
    private boolean closed;

    public ClientPoolImpl(int poolSize, Class<? extends E> reconnectExc, boolean retryByDefault) {
        this.poolSize = poolSize;
        this.reconnectExc = reconnectExc;
        this.clients = new ArrayDeque<>(poolSize);
        this.currentSize = 0;
        this.closed = false;
        this.retryByDefault = retryByDefault;
    }

    @Override
    public <R> R run(RunAction<R, C, E> poolAction) throws E {
        return run(poolAction, retryByDefault);
    }

    @Override
    public <R> R run(RunAction<R, C, E> poolAction, boolean retry) throws E {
        return executeWithRetry(poolAction, retry);
    }

    @Override
    public void execute(ExecuteAction<C, E> action) throws E {
        execute(action, retryByDefault);
    }

    @Override
    public void execute(ExecuteAction<C, E> action, boolean retry) throws E {
        executeWithRetry(action, retry);
    }

    private <R> R executeWithRetry(RunAction<R, C, E> action, boolean retry) throws E {
        C client = get();
        try {
            return action.run(client);
        } catch (Exception exc) {
            if (retry && isConnectionException(exc)) {
                return handleRetry(action, client, exc);
            }
            throw exc;
        } finally {
            release(client);
        }
    }

    private void executeWithRetry(ExecuteAction<C, E> action, boolean retry) throws E {
        C client = get();
        try {
            action.run(client);
        } catch (Exception exc) {
            if (retry && isConnectionException(exc)) {
                handleRetry(action, client, exc);
            }
            throw exc;
        } finally {
            release(client);
        }
    }

    private <R> R handleRetry(RunAction<R, C, E> action, C client, Exception originalException)
            throws E {
        try {
            client = reconnect(client);
            return action.run(client);
        } catch (Exception e) {
            throw reconnectExc.cast(originalException);
        } finally {
            release(client);
        }
    }

    private void handleRetry(ExecuteAction<C, E> action, C client, Exception originalException)
            throws E {
        try {
            client = reconnect(client);
            action.run(client);
        } catch (Exception e) {
            throw reconnectExc.cast(originalException);
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
                    // wake every second in case this missed the signal
                    synchronized (signal) {
                        signal.wait(1000);
                    }
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while shutting down pool. Some clients may not be closed.", e);
        }
    }

    private C get() {
        Preconditions.checkState(!closed, "Cannot get a client from a closed pool");
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
                try {
                    // wake every second in case this missed the signal
                    signal.wait(1000);
                } catch (InterruptedException ignored) {

                }
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
}
