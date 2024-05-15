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
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/** A procedure to expire snapshots. */
public class ExpireSnapshotsProcedure extends ProcedureBase {

    @Override
    public String identifier() {
        return "expire_snapshots";
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING"), isOptional = false),
                @ArgumentHint(
                        name = "retain_max",
                        type = @DataTypeHint("INTEGER"),
                        isOptional = true),
                @ArgumentHint(
                        name = "retain_min",
                        type = @DataTypeHint("INTEGER"),
                        isOptional = true),
                @ArgumentHint(
                        name = "older_than",
                        type = @DataTypeHint(value = "STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "max_deletes",
                        type = @DataTypeHint("INTEGER"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            Integer retainMax,
            Integer retainMin,
            String olderThanStr,
            Integer maxDeletes)
            throws Catalog.TableNotExistException {
        ExpireSnapshots expireSnapshots = table(tableId).newExpireSnapshots();
        if (retainMax != null) {
            expireSnapshots.retainMax(retainMax);
        }
        if (retainMin != null) {
            expireSnapshots.retainMin(retainMin);
        }
        if (olderThanStr != null) {
            expireSnapshots.olderThanMills(
                    DateTimeUtils.parseTimestampData(olderThanStr, 3, DateTimeUtils.LOCAL_TZ)
                            .getMillisecond());
        }
        if (maxDeletes != null) {
            expireSnapshots.maxDeletes(maxDeletes);
        }
        return new String[] {expireSnapshots.expire() + ""};
    }
}
