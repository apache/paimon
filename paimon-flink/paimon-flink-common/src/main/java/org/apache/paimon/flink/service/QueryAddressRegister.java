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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.net.InetSocketAddress;
import java.util.TreeMap;

import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;

/** Operator for address server to register addresses to {@link ServiceManager}. */
public class QueryAddressRegister implements Sink<InternalRow> {
    private final ServiceManager serviceManager;

    public QueryAddressRegister(Table table) {
        this.serviceManager = ((FileStoreTable) table).store().newServiceManager();
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public SinkWriter<InternalRow> createWriter(InitContext context) {
        return new QueryAddressRegisterSinkWriter(serviceManager);
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.18-.
     */
    public SinkWriter<InternalRow> createWriter(WriterInitContext context) {
        return new QueryAddressRegisterSinkWriter(serviceManager);
    }

    private static class QueryAddressRegisterSinkWriter implements SinkWriter<InternalRow> {
        private final ServiceManager serviceManager;

        private final TreeMap<Integer, InetSocketAddress> executors;

        private int numberExecutors;

        private QueryAddressRegisterSinkWriter(ServiceManager serviceManager) {
            this.serviceManager = serviceManager;
            this.executors = new TreeMap<>();
        }

        @Override
        public void write(InternalRow row, Context context) {
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
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {
            serviceManager.deleteService(PRIMARY_KEY_LOOKUP);
        }
    }
}
