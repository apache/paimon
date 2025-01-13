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

import org.apache.paimon.catalog.Identifier;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

/** Utils for hive alter table. */
public class HiveAlterTableUtils {

    public static void alterTable(IMetaStoreClient client, Identifier identifier, Table table)
            throws TException {
        try {
            alterTableWithEnv(client, identifier, table);
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            alterTableWithoutEnv(client, identifier, table);
        }
    }

    private static void alterTableWithEnv(
            IMetaStoreClient client, Identifier identifier, Table table) throws TException {
        EnvironmentContext environmentContext = new EnvironmentContext();
        environmentContext.putToProperties(StatsSetupConst.CASCADE, "true");
        environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, "false");
        client.alter_table_with_environmentContext(
                identifier.getDatabaseName(), identifier.getTableName(), table, environmentContext);
    }

    private static void alterTableWithoutEnv(
            IMetaStoreClient client, Identifier identifier, Table table) throws TException {
        client.alter_table(identifier.getDatabaseName(), identifier.getTableName(), table, true);
    }
}
