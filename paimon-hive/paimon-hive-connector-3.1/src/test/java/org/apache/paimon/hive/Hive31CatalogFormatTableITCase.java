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

import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_TXN_MANAGER;

/** IT cases for using Paimon {@link HiveCatalog} together with Paimon Hive 3.1 connector. */
public class Hive31CatalogFormatTableITCase extends HiveCatalogFormatTableITCaseBase {

    @HiveRunnerSetup
    private static final HiveRunnerConfig CONFIG =
            new HiveRunnerConfig() {
                {
                    // catalog lock needs txn manager
                    // hive-3.x requires a proper txn manager to create ACID table
                    getHiveConfSystemOverride()
                            .put(HIVE_TXN_MANAGER.varname, DbTxnManager.class.getName());
                    getHiveConfSystemOverride().put(HIVE_SUPPORT_CONCURRENCY.varname, "true");
                    // tell TxnHandler to prepare txn DB
                    getHiveConfSystemOverride().put(HIVE_IN_TEST.varname, "true");
                }
            };
}
