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
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * An interface that allows file store to use global lock to some transaction-related things.
 *
 * @since 0.4.0
 */
@Public
public interface Lock extends AutoCloseable {

    /** Run with lock. */
    <T> T runWithLock(Callable<T> callable) throws Exception;

    /** A factory to create {@link Lock}. */
    interface Factory extends Serializable {
        Lock create();
    }

    static Factory factory(
            @Nullable CatalogLockFactory lockFactory,
            @Nullable CatalogLockContext lockContext,
            Identifier tablePath) {
        return lockFactory == null
                ? new EmptyFactory()
                : new LockFactory(lockFactory, lockContext, tablePath);
    }

    static Factory emptyFactory() {
        return new EmptyFactory();
    }

    /** A {@link Factory} creating lock from catalog. */
    class LockFactory implements Factory {

        private static final long serialVersionUID = 1L;

        private final CatalogLockFactory lockFactory;
        private final CatalogLockContext lockContext;
        private final Identifier tablePath;

        public LockFactory(
                CatalogLockFactory lockFactory,
                CatalogLockContext lockContext,
                Identifier tablePath) {
            this.lockFactory = lockFactory;
            this.lockContext = lockContext;
            this.tablePath = tablePath;
        }

        @Override
        public Lock create() {
            return fromCatalog(lockFactory.createLock(lockContext), tablePath);
        }
    }

    /** A {@link Factory} creating empty lock. */
    class EmptyFactory implements Factory {

        private static final long serialVersionUID = 1L;

        @Override
        public Lock create() {
            return new EmptyLock();
        }
    }

    /** An empty lock. */
    class EmptyLock implements Lock {
        @Override
        public <T> T runWithLock(Callable<T> callable) throws Exception {
            return callable.call();
        }

        @Override
        public void close() {}
    }

    static Lock fromCatalog(CatalogLock lock, Identifier tablePath) {
        if (lock == null) {
            return new EmptyLock();
        }
        return new CatalogLockImpl(lock, tablePath);
    }

    /** A {@link Lock} to wrap {@link CatalogLock}. */
    class CatalogLockImpl implements Lock {

        private final CatalogLock catalogLock;
        private final Identifier tablePath;

        private CatalogLockImpl(CatalogLock catalogLock, Identifier tablePath) {
            this.catalogLock = catalogLock;
            this.tablePath = tablePath;
        }

        @Override
        public <T> T runWithLock(Callable<T> callable) throws Exception {
            return catalogLock.runWithLock(
                    tablePath.getDatabaseName(), tablePath.getObjectName(), callable);
        }

        @Override
        public void close() throws Exception {
            this.catalogLock.close();
        }
    }
}
