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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.schema.SchemaValidation;

import org.apache.flink.table.procedure.ProcedureContext;

import java.util.HashMap;
import java.util.Map;

/** A procedure to expire snapshots. */
public class ExpireSnapshotsProcedure extends ProcedureBase {

    @Override
    public String identifier() {
        return "expire_snapshots";
    }

    public String[] call(ProcedureContext procedureContext, String tableId, int retainMax)
            throws Catalog.TableNotExistException {
        Map<String, String> options = new HashMap<>(table(tableId).options());
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), String.valueOf(retainMax));
        if (!options.containsKey(CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key())) {
            options.put(CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key(), String.valueOf(retainMax));
        }
        CoreOptions newOption = new CoreOptions(options);
        SchemaValidation.validateSnapshotAndChangelogRetainOption(newOption);

        return new String[] {String.valueOf(table(tableId).newExpireSnapshots(newOption).expire())};
    }
}
