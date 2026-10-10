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
import org.apache.paimon.operation.Lock;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.concurrent.Callable;

/** Compatibility SPI for catalog lock factories. Runtime operations use {@link Lock}. */
@Public
public interface CatalogLock extends Lock, Closeable {

    <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception;

    /** Bind table incarnation and writer identity when the backend requires them. */
    default <T> T runWithLock(
            Identifier identifier,
            @Nullable String tableUuid,
            @Nullable String commitUser,
            Callable<T> callable)
            throws Exception {
        return runWithLock(identifier.getDatabaseName(), identifier.getObjectName(), callable);
    }

    @Override
    default <T> T runWithLock(Callable<T> callable) throws Exception {
        throw new UnsupportedOperationException("A catalog lock requires a table identifier.");
    }
}
