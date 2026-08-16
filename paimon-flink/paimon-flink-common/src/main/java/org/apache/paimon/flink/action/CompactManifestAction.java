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

import org.apache.paimon.flink.procedure.CompactManifestProcedure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Compact manifest action for Flink. */
public class CompactManifestAction extends ActionBase implements LocalAction {

    private static final Logger LOG = LoggerFactory.getLogger(CompactManifestAction.class);

    private final String database;
    private final String table;
    private final String options;
    private final Boolean dryRun;
    private final Boolean manifestSortEnabled;
    private final String manifestSortPartitionField;
    private final String manifestSortMaxRewriteSize;

    public CompactManifestAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            String options,
            Boolean dryRun,
            Boolean manifestSortEnabled,
            String manifestSortPartitionField,
            String manifestSortMaxRewriteSize) {
        super(catalogConfig);
        this.database = database;
        this.table = table;
        this.options = options;
        this.dryRun = dryRun;
        this.manifestSortEnabled = manifestSortEnabled;
        this.manifestSortPartitionField = manifestSortPartitionField;
        this.manifestSortMaxRewriteSize = manifestSortMaxRewriteSize;
    }

    @Override
    public void executeLocally() throws Exception {
        CompactManifestProcedure procedure = new CompactManifestProcedure();
        procedure.withCatalog(catalog);
        String[] results =
                procedure.call(
                        null,
                        database + "." + table,
                        options,
                        dryRun,
                        manifestSortEnabled,
                        manifestSortPartitionField,
                        manifestSortMaxRewriteSize);
        for (String result : results) {
            LOG.info(result);
        }
    }
}
