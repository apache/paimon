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

package org.apache.paimon.jdbc;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.Striped;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcRenameLockOrderTest {
    @TempDir java.nio.file.Path tempDir;

    @Test
    void independentRenamesCompleteWhenIdentifierLocksShareStripes() throws Exception {
        Options options = new Options();
        options.set(CatalogOptions.URI, "jdbc:sqlite:" + tempDir.resolve("catalog.db"));
        options.set(JdbcCatalogOptions.LOCK_ENABLED, false);
        String warehouse = tempDir.resolve("warehouse").toUri().toString();
        try (JdbcCatalog catalog =
                new JdbcCatalog(
                        LocalFileIO.create(),
                        "rename-stripes",
                        CatalogContext.create(options),
                        warehouse)) {
            catalog.createDatabase("db", false);
            Field field = JdbcCatalog.class.getDeclaredField("LOCAL_LOCKS");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Striped<Lock> stripes = (Striped<Lock>) field.get(null);
            Method lockKey = JdbcCatalog.class.getDeclaredMethod("lockKey", Identifier.class);
            lockKey.setAccessible(true);
            Map<Lock, List<Identifier>> groups = new IdentityHashMap<>();
            for (int i = 0; i < 256; i++) {
                Identifier id = Identifier.create("db", String.format(Locale.ROOT, "t%04d", i));
                Lock stripe = stripes.get(lockKey.invoke(catalog, id));
                groups.computeIfAbsent(stripe, ignored -> new ArrayList<>()).add(id);
            }
            List<Identifier> firstGroup = null;
            List<Identifier> secondGroup = null;
            for (List<Identifier> a : groups.values()) {
                for (List<Identifier> b : groups.values()) {
                    if (a != b
                            && a.size() > 1
                            && b.size() > 1
                            && a.get(0)
                                            .getObjectName()
                                            .compareTo(b.get(b.size() - 1).getObjectName())
                                    < 0
                            && b.get(0)
                                            .getObjectName()
                                            .compareTo(a.get(a.size() - 1).getObjectName())
                                    < 0) {
                        firstGroup = a;
                        secondGroup = b;
                        break;
                    }
                }
                if (firstGroup != null) {
                    break;
                }
            }
            assertThat(firstGroup).isNotNull();
            List<Lock> ordered = new ArrayList<>();
            for (Lock stripe :
                    stripes.bulkGet(
                            Arrays.asList(
                                    lockKey.invoke(catalog, firstGroup.get(0)),
                                    lockKey.invoke(catalog, secondGroup.get(0))))) {
                ordered.add(stripe);
            }
            if (stripes.get(lockKey.invoke(catalog, firstGroup.get(0))) != ordered.get(0)) {
                List<Identifier> swap = firstGroup;
                firstGroup = secondGroup;
                secondGroup = swap;
            }
            Identifier fromFirst = firstGroup.get(0);
            Identifier toFirst = secondGroup.get(secondGroup.size() - 1);
            Identifier fromSecond = secondGroup.get(0);
            Identifier toSecond = firstGroup.get(firstGroup.size() - 1);
            Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
            catalog.createTable(fromFirst, schema, false);
            catalog.createTable(fromSecond, schema, false);
            ReentrantLock low = (ReentrantLock) ordered.get(0);
            ReentrantLock high = (ReentrantLock) ordered.get(1);
            CountDownLatch finished = new CountDownLatch(2);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread first = rename(catalog, fromFirst, toFirst, finished, failure);
            Thread second = rename(catalog, fromSecond, toSecond, finished, failure);
            high.lock();
            try {
                second.start();
                await(() -> high.hasQueuedThread(second));
                first.start();
                await(() -> high.hasQueuedThread(first) || low.hasQueuedThread(first));
                high.unlock();
                assertThat(finished.await(5, TimeUnit.SECONDS))
                        .withFailMessage("Independent JDBC renames did not finish")
                        .isTrue();
                first.join();
                second.join();
                assertThat(failure.get()).isNull();
                assertThat(catalog.listTables("db"))
                        .containsExactlyInAnyOrder(
                                toFirst.getObjectName(), toSecond.getObjectName());
                assertThat(catalog.getTable(toFirst).rowType().getFieldNames())
                        .containsExactly("id");
                assertThat(catalog.getTable(toSecond).rowType().getFieldNames())
                        .containsExactly("id");
            } finally {
                if (high.isHeldByCurrentThread()) {
                    high.unlock();
                }
            }
        }
    }

    private static Thread rename(
            JdbcCatalog catalog,
            Identifier from,
            Identifier to,
            CountDownLatch finished,
            AtomicReference<Throwable> failure) {
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                catalog.renameTable(from, to, false);
                            } catch (Throwable e) {
                                failure.compareAndSet(null, e);
                            } finally {
                                finished.countDown();
                            }
                        });
        // A failing regression fork must exit even if the renames deadlock in lock().
        thread.setDaemon(true);
        return thread;
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
