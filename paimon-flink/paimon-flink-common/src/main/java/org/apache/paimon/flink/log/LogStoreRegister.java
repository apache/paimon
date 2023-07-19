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

package org.apache.paimon.flink.log;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.options.Options;

import java.util.Collections;
import java.util.Map;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM_AUTO_REGISTER;
import static org.apache.paimon.flink.FlinkConnectorOptions.NONE;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * {@link LogStoreRegister} will register and unregister topic for a Paimon table, you can implement
 * it for customized log system management.
 */
public interface LogStoreRegister {
    /** Register topic in log system for the table. */
    Map<String, String> registerTopic();

    /** Unregister topic in log system for the table. */
    void unRegisterTopic();

    static Map<String, String> registerLogSystem(
            Catalog catalog,
            Identifier identifier,
            Map<String, String> options,
            ClassLoader classLoader) {
        Options tableOptions = Options.fromMap(options);
        if (tableOptions.get(LOG_SYSTEM_AUTO_REGISTER)) {
            String logStore = tableOptions.get(LOG_SYSTEM);
            checkArgument(
                    !tableOptions.get(LOG_SYSTEM).equalsIgnoreCase(NONE),
                    String.format(
                            "%s must be configured when you use log system register.",
                            LOG_SYSTEM.key()));
            if (catalog.tableExists(identifier)) {
                return Collections.emptyMap();
            }
            LogStoreRegister logStoreRegister =
                    getLogStoreRegister(identifier, classLoader, tableOptions, logStore);
            return logStoreRegister.registerTopic();
        }
        return Collections.emptyMap();
    }

    static void unRegisterLogSystem(
            Identifier identifier, Map<String, String> options, ClassLoader classLoader) {
        Options tableOptions = Options.fromMap(options);
        if (tableOptions.get(LOG_SYSTEM_AUTO_REGISTER)) {
            String logStore = tableOptions.get(LOG_SYSTEM);
            checkArgument(
                    !tableOptions.get(LOG_SYSTEM).equalsIgnoreCase(NONE),
                    String.format(
                            "%s must be configured when you use log system register.",
                            LOG_SYSTEM.key()));
            LogStoreRegister logStoreRegister =
                    getLogStoreRegister(identifier, classLoader, tableOptions, logStore);
            logStoreRegister.unRegisterTopic();
        }
    }

    static LogStoreRegister getLogStoreRegister(
            Identifier identifier, ClassLoader classLoader, Options tableOptions, String logStore) {
        LogStoreTableFactory registerFactory =
                FactoryUtil.discoverFactory(classLoader, LogStoreTableFactory.class, logStore);
        return registerFactory.createRegister(
                new LogStoreTableFactory.RegisterContext() {
                    @Override
                    public Options getOptions() {
                        return tableOptions;
                    }

                    @Override
                    public Identifier getIdentifier() {
                        return identifier;
                    }
                });
    }
}
