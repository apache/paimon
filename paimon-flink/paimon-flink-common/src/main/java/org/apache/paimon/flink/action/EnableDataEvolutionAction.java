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

import org.apache.paimon.append.dataevolution.DataEvolutionEnabler;
import org.apache.paimon.catalog.Identifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Enables data evolution on an existing append table, see {@link DataEvolutionEnabler}. */
public class EnableDataEvolutionAction extends ActionBase implements LocalAction {

    private static final Logger LOG = LoggerFactory.getLogger(EnableDataEvolutionAction.class);

    private final Identifier identifier;
    private final boolean dryRun;

    public EnableDataEvolutionAction(
            Map<String, String> catalogConfig,
            String databaseName,
            String tableName,
            boolean dryRun) {
        super(catalogConfig);
        this.identifier = Identifier.create(databaseName, tableName);
        this.dryRun = dryRun;
    }

    @Override
    public void executeLocally() throws Exception {
        DataEvolutionEnabler.Result result =
                new DataEvolutionEnabler(catalog, identifier).run(dryRun);
        LOG.info(result.describe(identifier));
        System.out.println(result.describe(identifier));
    }
}
