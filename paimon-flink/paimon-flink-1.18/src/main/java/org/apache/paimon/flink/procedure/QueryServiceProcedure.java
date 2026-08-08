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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.service.QueryService;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.GlobalIndexQuerySnapshotLease;
import org.apache.paimon.utils.TimeUtils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Flink 1.18 positional-argument compatibility for the query-service procedure.
 *
 * <p>Flink 1.18 cannot omit optional procedure arguments declared with {@code ProcedureHint}, so
 * this class keeps the original two-argument primary-key service and adds positional global-index
 * overloads. Newer Flink versions use the annotated implementation from paimon-flink-common.
 */
public class QueryServiceProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "query_service";

    public String[] call(ProcedureContext procedureContext, String tableId, Integer parallelism)
            throws Exception {
        return call(procedureContext, tableId, parallelism, null, null, null, null);
    }

    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            Integer parallelism,
            String lookupKey,
            String valueFields)
            throws Exception {
        return call(procedureContext, tableId, parallelism, lookupKey, valueFields, null, null);
    }

    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            Integer parallelism,
            String lookupKey,
            String valueFields,
            String consumerId,
            String leaseGracePeriod)
            throws Exception {
        if ((lookupKey == null) != (valueFields == null)) {
            throw new IllegalArgumentException(
                    "Arguments lookup_key and value_fields must be specified together.");
        }
        if (lookupKey == null && (consumerId != null || leaseGracePeriod != null)) {
            throw new IllegalArgumentException(
                    "Arguments consumer_id and lease_grace_period require lookup_key and value_fields.");
        }
        if (consumerId != null) {
            GlobalIndexQuerySnapshotLease.validateConsumerIdPrefix(consumerId);
        }

        Table table = catalog.getTable(Identifier.fromString(tableId));
        StreamExecutionEnvironment env = procedureContext.getExecutionEnvironment();
        if (lookupKey == null) {
            QueryService.build(env, table, parallelism);
            return execute(env, IDENTIFIER);
        }

        List<String> values =
                Arrays.stream(valueFields.split(","))
                        .map(String::trim)
                        .filter(value -> !value.isEmpty())
                        .collect(Collectors.toList());
        if (consumerId == null && leaseGracePeriod == null) {
            QueryService.build(env, table, parallelism, lookupKey, values);
        } else {
            QueryService.build(
                    env,
                    table,
                    parallelism,
                    lookupKey,
                    values,
                    consumerId,
                    leaseGracePeriod == null
                            ? QueryService.DEFAULT_LEASE_GRACE_PERIOD
                            : TimeUtils.parseDuration(leaseGracePeriod));
        }
        return execute(env, IDENTIFIER);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
