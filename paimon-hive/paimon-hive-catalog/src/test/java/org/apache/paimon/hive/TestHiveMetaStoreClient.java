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
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.ArrayList;
import java.util.List;

/** A {@link HiveMetaStoreClient} to test custom Hive metastore client. */
public class TestHiveMetaStoreClient extends HiveMetaStoreClient implements IMetaStoreClient {

    public static final String MOCK_DATABASE = "test_mock_database";

    public TestHiveMetaStoreClient(HiveConf conf) throws MetaException {
        super(conf);
    }

    public TestHiveMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader)
            throws MetaException {
        super(conf, hookLoader);
    }

    public TestHiveMetaStoreClient(
            HiveConf conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded)
            throws MetaException {
        super(conf, hookLoader, allowEmbedded);
    }

    @Override
    public List<String> getAllDatabases() throws MetaException {
        List<String> result = new ArrayList<>(super.getAllDatabases());
        result.add(MOCK_DATABASE);
        return result;
    }
}
