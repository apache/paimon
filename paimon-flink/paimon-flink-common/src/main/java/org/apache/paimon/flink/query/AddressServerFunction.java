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

package org.apache.paimon.flink.query;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ExecutorThreadFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Operator for address server. */
public class AddressServerFunction extends RichSinkFunction<InternalRow> {

    private final Catalog.Loader catalogLoader;

    private transient int numberExecutors;
    private transient Map<Integer, InetSocketAddress> executors;
    private transient ConcurrentHashMap<Identifier, FileStoreTable> tables;
    private transient Catalog catalog;
    private transient ScheduledExecutorService addressRegister;

    public AddressServerFunction(Catalog.Loader catalogLoader) {
        this.catalogLoader = catalogLoader;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.executors = new HashMap<>();
        this.tables = new ConcurrentHashMap<>();
        this.catalog = catalogLoader.load();
        this.addressRegister =
                Executors.newScheduledThreadPool(
                        1,
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "-address-register"));
        this.addressRegister.schedule(this::refreshRegistry, 1, TimeUnit.MINUTES);
    }

    @Override
    public void close() {
        addressRegister.shutdownNow();
    }

    private void refreshRegistry() {
        tables.values().forEach(table -> table.registerService("hostname", -1));
    }

    @Override
    public void invoke(InternalRow row, SinkFunction.Context context) throws Exception {
        String database = row.getString(0).toString();
        String table = row.getString(1).toString();
        Identifier identifier = new Identifier(database, table);
        tables.computeIfAbsent(
                identifier,
                k -> {
                    try {
                        return (FileStoreTable) catalog.getTable(identifier);
                    } catch (Catalog.TableNotExistException e) {
                        throw new RuntimeException(e);
                    }
                });

        int numberExecutors = row.getInt(2);
        if (this.numberExecutors != 0 && this.numberExecutors != numberExecutors) {
            throw new IllegalArgumentException(
                    String.format(
                            "Number Executors can not be changed! Old %s , New %s .",
                            this.numberExecutors, numberExecutors));
        }
        this.numberExecutors = numberExecutors;

        int executorId = row.getInt(3);
        String hostname = row.getString(4).toString();
        int port = row.getInt(5);

        executors.put(executorId, new InetSocketAddress(hostname, port));
    }

    public int numberExecutors() {
        return numberExecutors;
    }

    public Map<Integer, InetSocketAddress> executors() {
        return executors;
    }

    public static int computeHash(String database, String table, BinaryRow partition, int bucket) {
        return Math.abs(Objects.hash(database, table, partition, bucket));
    }
}
