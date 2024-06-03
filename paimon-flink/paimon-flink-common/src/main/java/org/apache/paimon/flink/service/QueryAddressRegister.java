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

package org.apache.paimon.flink.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.net.InetSocketAddress;
import java.util.TreeMap;

import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;

/** Operator for address server to register addresses to {@link ServiceManager}. */
public class QueryAddressRegister extends RichSinkFunction<InternalRow> {

    private final ServiceManager serviceManager;

    private transient int numberExecutors;
    private transient TreeMap<Integer, InetSocketAddress> executors;

    public QueryAddressRegister(Table table) {
        this.serviceManager = ((FileStoreTable) table).store().newServiceManager();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.executors = new TreeMap<>();
    }

    @Override
    public void invoke(InternalRow row, SinkFunction.Context context) {
        int numberExecutors = row.getInt(0);
        if (this.numberExecutors != 0 && this.numberExecutors != numberExecutors) {
            throw new IllegalArgumentException(
                    String.format(
                            "Number Executors can not be changed! Old %s , New %s .",
                            this.numberExecutors, numberExecutors));
        }
        this.numberExecutors = numberExecutors;

        int executorId = row.getInt(1);
        String hostname = row.getString(2).toString();
        int port = row.getInt(3);

        executors.put(executorId, new InetSocketAddress(hostname, port));

        if (executors.size() == numberExecutors) {
            serviceManager.resetService(
                    PRIMARY_KEY_LOOKUP, executors.values().toArray(new InetSocketAddress[0]));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        serviceManager.deleteService(PRIMARY_KEY_LOOKUP);
    }
}
