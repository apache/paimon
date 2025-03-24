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

import org.apache.paimon.flink.procedure.ExpireChangelogsProcedure;

import org.apache.flink.table.procedure.DefaultProcedureContext;

import java.util.Map;

/** Expire changelogs action for Flink. */
public class ExpireChangelogsAction extends ActionBase {
    private final String database;
    private final String table;
    private final Integer retainMax;
    private final Integer retainMin;
    private final String olderThan;
    private final Integer maxDeletes;
    private final Boolean deleteAll;

    public ExpireChangelogsAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            Integer retainMax,
            Integer retainMin,
            String olderThan,
            Integer maxDeletes,
            Boolean deleteAll) {
        super(catalogConfig);
        this.database = database;
        this.table = table;
        this.retainMax = retainMax;
        this.retainMin = retainMin;
        this.olderThan = olderThan;
        this.maxDeletes = maxDeletes;
        this.deleteAll = deleteAll;
    }

    public void run() throws Exception {
        ExpireChangelogsProcedure expireSnapshotsProcedure = new ExpireChangelogsProcedure();
        expireSnapshotsProcedure.withCatalog(catalog);
        expireSnapshotsProcedure.call(
                new DefaultProcedureContext(env),
                database + "." + table,
                retainMax,
                retainMin,
                olderThan,
                maxDeletes,
                deleteAll);
    }
}
