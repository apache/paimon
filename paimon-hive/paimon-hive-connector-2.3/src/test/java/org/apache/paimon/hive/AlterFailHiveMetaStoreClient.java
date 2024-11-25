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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

/** A {@link HiveMetaStoreClient} to test altering table failed in Hive metastore client. */
public class AlterFailHiveMetaStoreClient extends HiveMetaStoreClient implements IMetaStoreClient {

    public AlterFailHiveMetaStoreClient(HiveConf conf) throws MetaException {
        super(conf);
    }

    public AlterFailHiveMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader)
            throws MetaException {
        super(conf, hookLoader);
    }

    public AlterFailHiveMetaStoreClient(
            HiveConf conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded)
            throws MetaException {
        super(conf, hookLoader, allowEmbedded);
    }

    @Override
    public void alter_table(
            String defaultDatabaseName, String tblName, Table table, boolean cascade)
            throws InvalidOperationException, MetaException, TException {
        throw new TException();
    }

    @Override
    public void alter_table_with_environmentContext(
            String defaultDatabaseName, String tblName, Table table, EnvironmentContext env)
            throws InvalidOperationException, MetaException, TException {
        throw new TException();
    }
}
