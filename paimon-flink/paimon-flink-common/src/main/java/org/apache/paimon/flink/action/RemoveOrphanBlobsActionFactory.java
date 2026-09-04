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

import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory to create {@link RemoveOrphanBlobsAction}. */
public class RemoveOrphanBlobsActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "remove_orphan_blobs";
    private static final String OLDER_THAN = "older_than";
    private static final String DRY_RUN = "dry_run";
    private static final String PARALLELISM = "parallelism";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        boolean dryRun = false;
        if (params.has(DRY_RUN)) {
            String dryRunValue = params.get(DRY_RUN);
            checkArgument(
                    "true".equalsIgnoreCase(dryRunValue) || "false".equalsIgnoreCase(dryRunValue),
                    "Argument 'dry_run' must be either 'true' or 'false', but was '%s'.",
                    dryRunValue);
            dryRun = Boolean.parseBoolean(dryRunValue);
        }
        RemoveOrphanBlobsAction.parseParallelism(params.get(PARALLELISM));

        RemoveOrphanBlobsAction action =
                new RemoveOrphanBlobsAction(
                        params.getRequired(DATABASE),
                        params.get(TABLE),
                        params.get(PARALLELISM),
                        catalogConfigMap(params));

        if (params.has(OLDER_THAN)) {
            action.olderThan(params.get(OLDER_THAN));
        }

        if (dryRun) {
            action.dryRun();
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"remove_orphan_blobs\" removes unreferenced primary-key managed BLOB packs.");
        System.out.println();
        System.out.println("Syntax:");
        System.out.println(
                "  remove_orphan_blobs \\\n"
                        + "--warehouse <warehouse_path> \\\n"
                        + "--database <database_name> \\\n"
                        + "--table <table_name> \\\n"
                        + "[--older_than <timestamp>] \\\n"
                        + "[--dry_run <false/true>] \\\n"
                        + "[--parallelism <positive_integer>]");
        System.out.println();
        System.out.println(
                "To avoid deleting newly written packs, the default cutoff is 1 day before the action starts. "
                        + "'--older_than' sets the absolute cutoff timestamp; only packs with an earlier modification time are eligible. "
                        + "<timestamp> format: yyyy-MM-dd HH:mm:ss");
        System.out.println();
        System.out.println(
                "When '--dry_run true', calculate the orphan pack count and total bytes without deleting files. Default is false.");
        System.out.println();
        System.out.println(
                "'--parallelism' controls the parallelism of each table cleanup job. It must be greater than 0.");
        System.out.println();
        System.out.println(
                "If the table is null or *, all managed BLOB packs in all tables under the db will be cleaned up.");
        System.out.println();
    }
}
