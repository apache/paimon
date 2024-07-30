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

package org.apache.paimon.flink;

import org.apache.paimon.annotation.Documentation.ExcludeFromDocumentation;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

import java.time.Duration;

/** Options for flink catalog. */
public class FlinkCatalogOptions {
    @ExcludeFromDocumentation("Only for internal use")
    public static final ConfigOption<String> CATALOG_TYPE =
            ConfigOptions.key("catalog-type")
                    .stringType()
                    .defaultValue(FlinkCatalogFactory.IDENTIFIER);

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key("default-database")
                    .stringType()
                    .defaultValue(Catalog.DEFAULT_DATABASE);

    @ExcludeFromDocumentation("Confused without log system")
    public static final ConfigOption<Boolean> LOG_SYSTEM_AUTO_REGISTER =
            ConfigOptions.key("log.system.auto-register")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, the register will automatically create and delete a topic in log system for Paimon table. Default kafka log store register "
                                    + "is supported, users can implement customized register for log system, for example, create a new class which extends "
                                    + "KafkaLogStoreFactory and return a customized LogStoreRegister for their kafka cluster to create/delete topics.");

    @ExcludeFromDocumentation("Confused without log system")
    public static final ConfigOption<Duration> REGISTER_TIMEOUT =
            ConfigOptions.key("log.system.auto-register-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The timeout for register to create or delete topic in log system.");

    public static final ConfigOption<Boolean> DISABLE_CREATE_TABLE_IN_DEFAULT_DB =
            ConfigOptions.key("disable-create-table-in-default-db")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, creating table in default database is not allowed. Default is false.");
}
