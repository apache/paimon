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

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.Identifier;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the unified operation lock and compatibility with catalog lock factories. */
class LockTest {
    @Test
    void nestedPublicationUsesOneScopeAndChecksValidity() throws Exception {
        List<String> events = new ArrayList<>();
        CatalogLock backend =
                new CatalogLock() {
                    @Override
                    public <T> T runWithLock(String database, String table, Callable<T> action)
                            throws Exception {
                        assertThat(database).isEqualTo("db");
                        assertThat(table).isEqualTo("table");
                        events.add("acquire");
                        try {
                            return action.call();
                        } finally {
                            events.add("release");
                        }
                    }

                    @Override
                    public void ensureValid() {
                        events.add("validate");
                    }

                    @Override
                    public void close() {
                        events.add("close");
                    }
                };
        try (Lock lock =
                Lock.fromCatalog(backend, Identifier.create("db", "table"), "table-id", "writer")) {
            lock.runWithLock(
                    () -> {
                        events.add("head");
                        return lock.runWithLock(
                                () -> {
                                    events.add("publish");
                                    return true;
                                });
                    });
            lock.runWithLock(
                    () -> {
                        events.add("next");
                        return true;
                    });
        }
        assertThat(events)
                .isEqualTo(
                        Arrays.asList(
                                "acquire",
                                "validate",
                                "head",
                                "validate",
                                "publish",
                                "release",
                                "acquire",
                                "validate",
                                "next",
                                "release",
                                "close"));
    }

    @Test
    void expiredLeasePreventsNestedPublicationAndScopeIsReleased() throws Exception {
        List<String> events = new ArrayList<>();
        AtomicBoolean expired = new AtomicBoolean();
        Lock backend =
                new Lock() {
                    @Override
                    public <T> T runWithLock(Callable<T> action) throws Exception {
                        events.add("acquire");
                        try {
                            return action.call();
                        } finally {
                            events.add("release");
                        }
                    }

                    @Override
                    public void ensureValid() {
                        if (expired.get()) {
                            throw new IllegalStateException("Expired");
                        }
                    }

                    @Override
                    public void close() {}
                };
        Lock lock = Lock.reentrant(backend);
        assertThatThrownBy(
                        () ->
                                lock.runWithLock(
                                        () -> {
                                            expired.set(true);
                                            return lock.runWithLock(
                                                    () -> {
                                                        events.add("publish");
                                                        return null;
                                                    });
                                        }))
                .hasMessage("Expired");
        assertThat(events).containsExactly("acquire", "release");
        expired.set(false);
        lock.runWithLock(() -> null);
        assertThat(events).containsExactly("acquire", "release", "acquire", "release");
    }

    @Test
    void runtimeIdentityReachesBackend() throws Exception {
        Identifier identifier = new Identifier("db", "table", "dev");
        CatalogLock backend =
                new CatalogLock() {
                    @Override
                    public <T> T runWithLock(String database, String table, Callable<T> action) {
                        throw new AssertionError("Runtime identity lost");
                    }

                    @Override
                    public <T> T runWithLock(
                            Identifier actual, String uuid, String owner, Callable<T> action)
                            throws Exception {
                        assertThat(actual).isEqualTo(identifier);
                        assertThat(uuid).isEqualTo("table-id");
                        assertThat(owner).isEqualTo("writer");
                        return action.call();
                    }

                    @Override
                    public void close() {}
                };
        try (Lock lock = Lock.fromCatalog(backend, identifier, "table-id", "writer")) {
            assertThat(lock.runWithLock(() -> "result")).isEqualTo("result");
        }
    }
}
