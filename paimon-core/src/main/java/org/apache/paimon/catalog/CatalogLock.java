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

import org.apache.paimon.annotation.Public;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * An interface that allows source and sink to use global lock to some transaction-related things.
 *
 * @since 0.4.0
 */
@Public
public interface CatalogLock extends Closeable {

    /** Run with catalog lock. The caller should tell catalog the database and table name. */
    <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception;

    /**
     * Acquire a commit lease for the exact table UUID, branch and commit user. An empty result
     * means another writer holds the lease. The scope covers head refresh, validation, preparation
     * and publication; implementations must also gate writers that do not request leases.
     *
     * <p>This capability is separate from {@link #runWithLock}. Existing publication locks do not
     * automatically support a larger commit scope.
     */
    default Optional<CatalogCommitLock> acquireCommitLock(
            Identifier identifier, String tableUuid, String commitUser) throws Exception {
        throw new UnsupportedOperationException(
                "This catalog lock does not support commit leases.");
    }
}
