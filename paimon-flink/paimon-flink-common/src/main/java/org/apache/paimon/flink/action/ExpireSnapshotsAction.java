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

import org.apache.paimon.flink.procedure.ExpireSnapshotsProcedure;

import org.apache.flink.table.procedure.DefaultProcedureContext;

import java.util.Map;

/** Expire snapshots action for Flink. */
public class ExpireSnapshotsAction extends ActionBase {

    private final String database;
    private final String table;
    private final Integer retainMax;
    private final Integer retainMin;
    private final String olderThan;
    private final Integer maxDeletes;

    public ExpireSnapshotsAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            Integer retainMax,
            Integer retainMin,
            String olderThan,
            Integer maxDeletes) {
        super(catalogConfig);
        this.database = database;
        this.table = table;
        this.retainMax = retainMax;
        this.retainMin = retainMin;
        this.olderThan = olderThan;
        this.maxDeletes = maxDeletes;
    }

    public void run() throws Exception {
        ExpireSnapshotsProcedure expireSnapshotsProcedure = new ExpireSnapshotsProcedure();
        expireSnapshotsProcedure.withCatalog(catalog);
        expireSnapshotsProcedure.call(
                new DefaultProcedureContext(env),
                database + "." + table,
                retainMax,
                retainMin,
                olderThan,
                maxDeletes);
    }
}
