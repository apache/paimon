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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Clear consumers procedure. Usage:
 *
 * <pre><code>
 *  -- clear all consumers except the specified consumer in the table
 *  CALL sys.clear_consumers('tableId', 'consumerIds', true)
 *
 * -- clear all specified consumers in the table
 *  CALL sys.clear_consumers('tableId', 'consumerIds') or CALL sys.clear_consumers('tableId', 'consumerIds', false)
 *
 *  -- clear all consumers in the table
 *  CALL sys.clear_unspecified_consumers('tableId')
 * </code></pre>
 */
public class ClearConsumersProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "clear_consumers";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "consumer_ids",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "clear_unspecified",
                        type = @DataTypeHint("BOOLEAN"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String consumerIds,
            Boolean clearUnspecified)
            throws Catalog.TableNotExistException {
        FileStoreTable fileStoreTable =
                (FileStoreTable) catalog.getTable(Identifier.fromString(tableId));
        ConsumerManager consumerManager =
                new ConsumerManager(
                        fileStoreTable.fileIO(),
                        fileStoreTable.location(),
                        fileStoreTable.snapshotManager().branch());
        if (consumerIds != null) {
            List<String> specifiedConsumerIds =
                    Optional.of(consumerIds)
                            .map(s -> Arrays.asList(s.split(",")))
                            .orElse(Collections.emptyList());
            consumerManager.clearConsumers(
                    specifiedConsumerIds, Optional.ofNullable(clearUnspecified).orElse(false));
        } else {
            consumerManager.clearConsumers(null, null);
        }

        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
