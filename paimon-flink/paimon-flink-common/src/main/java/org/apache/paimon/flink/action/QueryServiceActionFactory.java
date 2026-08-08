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

package org.apache.paimon.flink.action;

import org.apache.paimon.flink.service.QueryService;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.GlobalIndexQuerySnapshotLease;
import org.apache.paimon.utils.TimeUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Factory to create QueryService Action. */
public class QueryServiceActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "query_service";

    public static final String PARALLELISM = "parallelism";

    public static final String LOOKUP_KEY = "lookup-key";

    public static final String VALUE_FIELDS = "value-fields";

    public static final String CONSUMER_ID = "consumer-id";

    public static final String LEASE_GRACE_PERIOD = "lease-grace-period";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Map<String, String> catalogConfig = catalogConfigMap(params);
        Map<String, String> tableConfig = optionalConfigMap(params, TABLE_CONF);
        String parallStr = params.get(PARALLELISM);
        int parallelism = parallStr == null ? 1 : Integer.parseInt(parallStr);
        String lookupKey = params.get(LOOKUP_KEY);
        String valueFieldsOption = params.get(VALUE_FIELDS);
        String consumerId = params.get(CONSUMER_ID);
        if (consumerId != null) {
            GlobalIndexQuerySnapshotLease.validateConsumerIdPrefix(consumerId);
        }
        if (lookupKey == null && (consumerId != null || params.has(LEASE_GRACE_PERIOD))) {
            throw new IllegalArgumentException(
                    "Options --consumer-id and --lease-grace-period require --lookup-key and --value-fields.");
        }
        Duration leaseGracePeriod =
                params.has(LEASE_GRACE_PERIOD)
                        ? TimeUtils.parseDuration(params.get(LEASE_GRACE_PERIOD))
                        : QueryService.DEFAULT_LEASE_GRACE_PERIOD;
        if ((lookupKey == null) != (valueFieldsOption == null)) {
            throw new IllegalArgumentException(
                    "Options --lookup-key and --value-fields must be specified together.");
        }
        List<String> valueFields =
                valueFieldsOption == null
                        ? null
                        : Arrays.stream(valueFieldsOption.split(","))
                                .map(String::trim)
                                .filter(value -> !value.isEmpty())
                                .collect(Collectors.toList());
        Action action =
                new TableActionBase(
                        params.getRequired(DATABASE), params.getRequired(TABLE), catalogConfig) {
                    @Override
                    public void run() throws Exception {
                        FileStoreTable serviceTable = (FileStoreTable) table.copy(tableConfig);
                        if (lookupKey == null) {
                            QueryService.build(env, serviceTable, parallelism);
                        } else {
                            String consumerIdPrefix =
                                    consumerId == null
                                            ? "global-index-query-"
                                                    + GlobalIndexQueryServiceUtils.querySpec(
                                                                    serviceTable,
                                                                    lookupKey,
                                                                    valueFields)
                                                            .serviceId()
                                            : consumerId;
                            QueryService.build(
                                    env,
                                    serviceTable,
                                    parallelism,
                                    lookupKey,
                                    valueFields,
                                    consumerIdPrefix,
                                    leaseGracePeriod);
                        }
                        execute("Query Service job");
                    }
                };
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"query_service\" runs a dedicated job starting query service for a table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  query_service \\\n"
                        + "--warehouse <warehouse-path> \\\n"
                        + "--database <database-name> \\\n"
                        + "--table <table-name> \\\n"
                        + "--parallelism <parallelism> \\\n"
                        + "[--lookup-key <unique-lookup-field> \\\n"
                        + " --value-fields <field>[,<field>...]] \\\n"
                        + "[--consumer-id <snapshot-lease-prefix>] \\\n"
                        + "[--lease-grace-period <duration>] \\\n"
                        + "[--catalog_conf <key>=<value> [--catalog_conf <key>=<value> ...]] \\\n"
                        + "[--table_conf <key>=<value> [--table_conf <key>=<value> ...]]");
    }
}
