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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.hive.RetryingMetaStoreClientFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

/**
 * Pool of Hive Metastore clients.
 *
 * <p>Mostly copied from iceberg.
 */
public class HiveClientPool extends ClientPool.ClientPoolImpl<IMetaStoreClient, TException> {

    private final HiveConf hiveConf;
    private final String clientClassName;

    public HiveClientPool(int poolSize, Configuration conf, String clientClassName) {
        // Do not allow retry by default as we rely on RetryingHiveClient
        super(poolSize, TTransportException.class, false);
        this.hiveConf = new HiveConf(conf, HiveClientPool.class);
        this.hiveConf.addResource(conf);
        this.clientClassName = clientClassName;
        // set ugi information to hms client
        try {
            this.run(client -> null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to Hive Metastore", e);
        }
    }

    @Override
    protected IMetaStoreClient newClient() {
        return new RetryingMetaStoreClientFactory().createClient(hiveConf, clientClassName);
    }

    @Override
    protected IMetaStoreClient reconnect(IMetaStoreClient client) {
        try {
            client.close();
            client.reconnect();
        } catch (MetaException e) {
            throw new RuntimeException("Failed to reconnect to Hive Metastore", e);
        }
        return client;
    }

    @Override
    protected boolean isConnectionException(Exception e) {
        return super.isConnectionException(e)
                || (e instanceof MetaException
                        && e.getMessage()
                                .contains(
                                        "Got exception: org.apache.thrift.transport.TTransportException"));
    }

    @Override
    protected void close(IMetaStoreClient client) {
        client.close();
    }

    @VisibleForTesting
    HiveConf hiveConf() {
        return hiveConf;
    }
}
