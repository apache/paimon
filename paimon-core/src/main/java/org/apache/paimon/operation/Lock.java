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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.Identifier;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;

/** A table lock covering an operation and its publication. */
@Public
public interface Lock extends AutoCloseable {

    <T> T runWithLock(Callable<T> callable) throws Exception;

    /** Fail before publication when a held lease has expired or renewal has failed. */
    default void ensureValid() {}

    static Lock empty() {
        return new EmptyLock();
    }

    /** An operation with no external lock configured. */
    class EmptyLock implements Lock {
        @Override
        public <T> T runWithLock(Callable<T> callable) throws Exception {
            return callable.call();
        }

        @Override
        public void close() {}
    }

    static Lock fromCatalog(CatalogLock lock, Identifier tablePath) {
        return fromCatalog(lock, tablePath, null, null);
    }

    static Lock fromCatalog(
            CatalogLock lock,
            Identifier tablePath,
            @Nullable String tableUuid,
            @Nullable String commitUser) {
        return lock == null
                ? empty()
                : reentrant(new CatalogLockImpl(lock, tablePath, tableUuid, commitUser));
    }

    /** Reuse the current scope for nested publication on the same thread. */
    static Lock reentrant(Lock lock) {
        return lock instanceof ReentrantLock ? lock : new ReentrantLock(lock);
    }

    /** Binds the legacy factory SPI to a table and writer. */
    class CatalogLockImpl implements Lock {
        private final CatalogLock catalogLock;
        private final Identifier tablePath;
        @Nullable private final String tableUuid;
        @Nullable private final String commitUser;

        private CatalogLockImpl(
                CatalogLock catalogLock,
                Identifier tablePath,
                @Nullable String tableUuid,
                @Nullable String commitUser) {
            this.catalogLock = catalogLock;
            this.tablePath = tablePath;
            this.tableUuid = tableUuid;
            this.commitUser = commitUser;
        }

        @Override
        public <T> T runWithLock(Callable<T> callable) throws Exception {
            return catalogLock.runWithLock(tablePath, tableUuid, commitUser, callable);
        }

        @Override
        public void ensureValid() {
            catalogLock.ensureValid();
        }

        @Override
        public void close() throws Exception {
            catalogLock.close();
        }
    }

    /** Keeps the outer operation's scope until preparation and publication both finish. */
    class ReentrantLock implements Lock {
        private final Lock delegate;
        private final ThreadLocal<Boolean> held = ThreadLocal.withInitial(() -> false);

        private ReentrantLock(Lock delegate) {
            this.delegate = delegate;
        }

        @Override
        public <T> T runWithLock(Callable<T> callable) throws Exception {
            if (held.get()) {
                delegate.ensureValid();
                return callable.call();
            }
            return delegate.runWithLock(
                    () -> {
                        held.set(true);
                        try {
                            delegate.ensureValid();
                            return callable.call();
                        } finally {
                            held.remove();
                        }
                    });
        }

        @Override
        public void ensureValid() {
            delegate.ensureValid();
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
