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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

/** A {@link HiveMetaStoreClient} Factory to test custom Hive metastore client. */
public class CustomConstructorMetastoreClient {

    /**
     * A {@link HiveMetaStoreClient} to test custom Hive metastore client with (HiveConf,
     * HiveMetaHookLoader) constructor.
     */
    public static class TwoParameterConstructorMetastoreClient extends HiveMetaStoreClient
            implements IMetaStoreClient {

        public TwoParameterConstructorMetastoreClient(
                Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
            super(conf, hookLoader);
        }
    }

    /**
     * A {@link HiveMetaStoreClient} to test custom Hive metastore client with (HiveConf)
     * constructor.
     */
    public static class OneParameterConstructorMetastoreClient extends HiveMetaStoreClient
            implements IMetaStoreClient {

        public OneParameterConstructorMetastoreClient(Configuration conf) throws MetaException {
            super(conf);
        }
    }

    public static class OtherParameterConstructorMetastoreClient extends HiveMetaStoreClient
            implements IMetaStoreClient {

        public OtherParameterConstructorMetastoreClient(HiveConf conf) throws MetaException {
            super(conf);
        }
    }
}
