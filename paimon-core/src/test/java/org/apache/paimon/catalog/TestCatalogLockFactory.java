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

import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;

import java.util.concurrent.Callable;

/** Records scopes acquired through the catalog lock factory SPI. */
public class TestCatalogLockFactory implements CatalogLockFactory {
    public static final String IDENTIFIER = "test-catalog-lock";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Lock createLock(
            CatalogLockContext context,
            Identifier identifier,
            String tableUuid,
            String commitUser,
            Options tableOptions) {
        Options options = context.options();
        return new Lock() {
            @Override
            public <T> T runWithLock(Callable<T> callable) throws Exception {
                options.set("test.lock.database", identifier.getDatabaseName());
                options.set("test.lock.table", identifier.getObjectName());
                options.set("test.lock.held", "true");
                try {
                    return callable.call();
                } finally {
                    options.set("test.lock.held", "false");
                }
            }

            @Override
            public void close() {
                options.set("test.lock.closed", "true");
            }
        };
    }
}
