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
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.Action.checkRequiredArgument;
import static org.apache.paimon.flink.action.Action.getTablePath;
import static org.apache.paimon.flink.action.Action.optionalConfigMap;

/** Create tag action for Flink. */
public class CreateTagAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(CreateTagAction.class);

    private final String tagName;
    private final long snapshotId;

    public CreateTagAction(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String tagName,
            long snapshotId) {
        super(warehouse, databaseName, tableName, catalogConfig);
        this.tagName = tagName;
        this.snapshotId = snapshotId;
    }

    public static Optional<Action> create(String[] args) {
        LOG.info("create-tag action args: {}", String.join(" ", args));

        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        if (params.has("help")) {
            printHelp();
            return Optional.empty();
        }

        checkRequiredArgument(params, "tag-name");
        checkRequiredArgument(params, "snapshot");

        Tuple3<String, String, String> tablePath = getTablePath(params);
        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");
        String tagName = params.get("tag-name");
        long snapshot = Long.parseLong(params.get("snapshot"));

        CreateTagAction action =
                new CreateTagAction(
                        tablePath.f0, tablePath.f1, tablePath.f2, catalogConfig, tagName, snapshot);
        return Optional.of(action);
    }

    private static void printHelp() {
        System.out.println("Action \"create-tag\" creates a tag from given snapshot.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  create-tag --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> --tag-name <tag-name> --snapshot <snapshot-id>");
        System.out.println();
    }

    @Override
    public void run() throws Exception {
        table.createTag(tagName, snapshotId);
    }
}
