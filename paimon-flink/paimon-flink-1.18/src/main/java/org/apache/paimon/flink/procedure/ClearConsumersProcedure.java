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
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.procedure.ProcedureContext;

import java.util.regex.Pattern;

/**
 * Clear consumers procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  -- clear all consumers in the table
 *  CALL sys.clear_consumers('tableId')
 *
 * -- clear some consumers in the table (accept regular expression)
 *  CALL sys.clear_consumers('tableId', 'includingConsumers')
 *
 * -- exclude some consumers (accept regular expression)
 *  CALL sys.clear_consumers('tableId', 'includingConsumers', 'excludingConsumers')
 * </code></pre>
 */
public class ClearConsumersProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "clear_consumers";

    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String includingConsumers,
            String excludingConsumers)
            throws Catalog.TableNotExistException {
        FileStoreTable fileStoreTable =
                (FileStoreTable) catalog.getTable(Identifier.fromString(tableId));
        ConsumerManager consumerManager =
                new ConsumerManager(
                        fileStoreTable.fileIO(),
                        fileStoreTable.location(),
                        fileStoreTable.snapshotManager().branch());

        Pattern includingPattern =
                StringUtils.isNullOrWhitespaceOnly(includingConsumers)
                        ? Pattern.compile(".*")
                        : Pattern.compile(includingConsumers);
        Pattern excludingPattern =
                StringUtils.isNullOrWhitespaceOnly(excludingConsumers)
                        ? null
                        : Pattern.compile(excludingConsumers);
        consumerManager.clearConsumers(includingPattern, excludingPattern);

        return new String[] {"Success"};
    }

    public String[] call(
            ProcedureContext procedureContext, String tableId, String includingConsumers)
            throws Catalog.TableNotExistException {
        return call(procedureContext, tableId, includingConsumers, null);
    }

    public String[] call(ProcedureContext procedureContext, String tableId)
            throws Catalog.TableNotExistException {
        return call(procedureContext, tableId, null, null);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
