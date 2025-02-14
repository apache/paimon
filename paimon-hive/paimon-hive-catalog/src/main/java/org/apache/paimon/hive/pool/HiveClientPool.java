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

package org.apache.paimon.hive.pool;

import org.apache.paimon.client.ClientPool;
import org.apache.paimon.hive.RetryingMetaStoreClientFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Pool of Hive Metastore clients.
 *
 * <p>Mostly copied from iceberg.
 */
public class HiveClientPool
        extends ClientPool.ClientPoolImpl<
                IMetaStoreClient, LinkedBlockingDeque<IMetaStoreClient>, TException> {

    private volatile LinkedBlockingDeque<IMetaStoreClient> clients;

    public HiveClientPool(int poolSize, Configuration conf, String clientClassName) {
        super(initPoolSupplier(poolSize, conf, clientClassName));
    }

    private static Supplier<LinkedBlockingDeque<IMetaStoreClient>> initPoolSupplier(
            int poolSize, Configuration conf, String clientClassName) {
        return () -> {
            LinkedBlockingDeque<IMetaStoreClient> clients = new LinkedBlockingDeque<>();
            for (int i = 0; i < poolSize; i++) {
                HiveConf hiveConf = new HiveConf(conf, HiveClientPool.class);
                hiveConf.addResource(conf);
                clients.add(
                        new RetryingMetaStoreClientFactory()
                                .createClient(hiveConf, clientClassName));
            }
            return clients;
        };
    }

    @Override
    protected void initPool(Supplier<LinkedBlockingDeque<IMetaStoreClient>> supplier) {
        this.clients = supplier.get();
    }

    @Override
    protected IMetaStoreClient getClient(long timeout, TimeUnit unit) throws InterruptedException {
        if (this.clients == null) {
            throw new IllegalStateException("Cannot get a client from a closed pool");
        }
        return this.clients.pollFirst(10, TimeUnit.SECONDS);
    }

    @Override
    protected void recycleClient(IMetaStoreClient client) {
        this.clients.addFirst(client);
    }

    @Override
    protected void closePool() {
        LinkedBlockingDeque<IMetaStoreClient> clients = this.clients;
        this.clients = null;
        if (clients != null) {
            List<IMetaStoreClient> drain = new ArrayList<>();
            clients.drainTo(drain);
            drain.forEach(IMetaStoreClient::close);
        }
    }
}
