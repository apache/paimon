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

package org.apache.paimon.hive;

import org.apache.paimon.client.ClientPool;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

/** Client pool for hive. */
public class HiveClientPool extends ClientPool.ClientPoolImpl<IMetaStoreClient, TException> {

    private final SerializableHiveConf hiveConf;
    private final String clientClassName;
    private final RetryingMetaStoreClientFactory retryingMetaStoreClientFactory;

    public HiveClientPool(int poolSize, SerializableHiveConf hiveConf, String clientClassName) {
        super(poolSize, TTransportException.class, true);
        this.hiveConf = hiveConf;
        this.clientClassName = clientClassName;
        this.retryingMetaStoreClientFactory = new RetryingMetaStoreClientFactory();
    }

    @Override
    protected IMetaStoreClient newClient() {
        return retryingMetaStoreClientFactory.createClient(hiveConf.conf(), clientClassName);
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
                || (e != null
                        && e instanceof MetaException
                        && e.getMessage()
                                .contains(
                                        "Got exception: org.apache.thrift.transport.TTransportException"));
    }

    @Override
    protected void close(IMetaStoreClient client) {
        client.close();
    }
}
