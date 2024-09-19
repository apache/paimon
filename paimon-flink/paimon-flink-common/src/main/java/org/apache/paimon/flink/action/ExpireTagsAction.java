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

import org.apache.paimon.flink.procedure.ExpireTagsProcedure;

import org.apache.flink.table.procedure.DefaultProcedureContext;

import java.util.Map;

/** Expire tags action for Flink. */
public class ExpireTagsAction extends ActionBase {

    private final String table;
    private final String olderThan;

    public ExpireTagsAction(
            String warehouse, String table, String olderThan, Map<String, String> catalogConfig) {
        super(warehouse, catalogConfig);
        this.table = table;
        this.olderThan = olderThan;
    }

    @Override
    public void run() throws Exception {
        ExpireTagsProcedure expireTagsProcedure = new ExpireTagsProcedure();
        expireTagsProcedure.withCatalog(catalog);
        expireTagsProcedure.call(new DefaultProcedureContext(env), table, olderThan);
    }
}
