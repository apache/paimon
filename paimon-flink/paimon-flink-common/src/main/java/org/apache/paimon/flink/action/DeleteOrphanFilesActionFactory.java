/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link DeleteOrphanFilesAction}. */
public class DeleteOrphanFilesActionFactory implements ActionFactory {
    @Override
    public String identifier() {
        return "delete-orphan";
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);
        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");

        DeleteOrphanFilesAction action =
                new DeleteOrphanFilesAction(
                        tablePath.f0, tablePath.f1, tablePath.f2, catalogConfig);

        if (params.has("older-than")) {
            action.olderThan(params.getLong("older-than"));
        }

        if (params.has("location")) {
            action.location(params.get("location"));
        }

        if (params.has("max-concurrent-deletes")) {
            action.maxConcurrentDeletes(params.getInt("max-concurrent-deletes"));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"delete-orphan\" deletes data from a table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  delete-orphan --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name>");
        System.out.println();

        System.out.println("alternative arguments:");
        System.out.println("--older-than <table-path> to represent the table path.");
        System.out.println(
                "--location  Directory to look for files, it is must the sub dir of the table location.");
        System.out.println(
                "--max-concurrent-deletes Size of the thread pool used for delete file actions.");
        System.out.println(
                "--deleteWith we can use the action to build a flink datastream job to custom deletion policy.");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "delete-orphan --path hdfs:///path/to/warehouse/test_db.db/test_table --database test_db --table test_table");
    }
}
