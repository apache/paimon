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

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;

import org.apache.hadoop.hive.conf.HiveConf;

import static org.apache.paimon.hive.HiveCatalogLock.LOCK_IDENTIFIER;
import static org.apache.paimon.hive.HiveCatalogLock.acquireTimeout;
import static org.apache.paimon.hive.HiveCatalogLock.checkMaxSleep;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Catalog lock factory for hive. */
public class HiveCatalogLockFactory implements CatalogLockFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public CatalogLock createLock(CatalogLockContext context) {
        checkArgument(context instanceof HiveCatalogLockContext);
        HiveCatalogLockContext hiveLockContext = (HiveCatalogLockContext) context;
        HiveConf conf = hiveLockContext.hiveConf().conf();
        return new HiveCatalogLock(
                HiveCatalog.createClient(conf, hiveLockContext.clientClassName()),
                checkMaxSleep(conf),
                acquireTimeout(conf));
    }

    @Override
    public String identifier() {
        return LOCK_IDENTIFIER;
    }
}
