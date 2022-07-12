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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.store.file.catalog.CatalogLock;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;

/** An interface that allows file store to use global lock to some transaction-related things. */
public interface Lock extends AutoCloseable {

    /** Run with lock. */
    <T> T runWithLock(Callable<T> callable) throws Exception;

    @Nullable
    static Lock fromCatalog(CatalogLock.Factory lockFactory, ObjectPath tablePath) {
        if (lockFactory == null) {
            return null;
        }

        return fromCatalog(lockFactory.create(), tablePath);
    }

    @Nullable
    static Lock fromCatalog(CatalogLock lock, ObjectPath tablePath) {
        if (lock == null) {
            return null;
        }
        return new CatalogLockImpl(lock, tablePath);
    }

    /** A {@link Lock} to wrap {@link CatalogLock}. */
    class CatalogLockImpl implements Lock {

        private final CatalogLock catalogLock;
        private final ObjectPath tablePath;

        private CatalogLockImpl(CatalogLock catalogLock, ObjectPath tablePath) {
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
