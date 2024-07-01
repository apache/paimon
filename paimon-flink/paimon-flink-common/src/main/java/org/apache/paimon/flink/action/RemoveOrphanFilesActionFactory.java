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

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link RemoveOrphanFilesAction}. */
public class RemoveOrphanFilesActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "remove_orphan_files";

    private static final String OLDER_THAN = "older_than";
    private static final String DRY_RUN = "dry_run";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);
        Map<String, String> catalogConfig = optionalConfigMap(params, CATALOG_CONF);

        RemoveOrphanFilesAction action =
                new RemoveOrphanFilesAction(
                        tablePath.f0, tablePath.f1, tablePath.f2, catalogConfig);
        
        if (params.has(OLDER_THAN)) {
            action.olderThan(params.get(OLDER_THAN));
        }

        if (params.has(DRY_RUN) && Boolean.parseBoolean(params.get(DRY_RUN))) {
            action.dryRun();
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"remove_orphan_files\" remove files which are not used by any snapshots or tags of a Paimon table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  remove_orphan_files --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> [--older_than <timestamp>] [--dry_run <false/true>]");

        System.out.println();
        System.out.println(
                "To avoid deleting newly written files, this action only deletes orphan files older than 1 day by default. "
                        + "The interval can be modified by '--older_than'. <timestamp> format: yyyy-MM-dd HH:mm:ss");

        System.out.println();
        System.out.println(
                "When '--dry_run true', view only orphan files, don't actually remove files. Default is false.");
        System.out.println();
    }
}
