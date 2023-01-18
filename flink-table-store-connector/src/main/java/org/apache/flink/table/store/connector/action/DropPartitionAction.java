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

package org.apache.flink.table.store.connector.action;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.TableCommit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.flink.table.store.connector.action.Action.getPartitions;
import static org.apache.flink.table.store.connector.action.Action.getTablePath;

/** Table drop partition action for Flink. */
public class DropPartitionAction implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(CompactAction.class);

    private final TableCommit commit;

    DropPartitionAction(Path tablePath, List<Map<String, String>> partitions) {
        FileStoreTable table = FileStoreTableFactory.create(tablePath);
        this.commit =
                table.newCommit(UUID.randomUUID().toString()).withOverwritePartitions(partitions);
    }

    public static Optional<Action> create(String[] args) {
        LOG.info("Drop partition job args: {}", String.join(" ", args));

        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        if (params.has("help")) {
            printHelp();
            return Optional.empty();
        }

        Path tablePath = getTablePath(params);

        if (tablePath == null) {
            return Optional.empty();
        }

        if (!params.has("partition")) {
            LOG.info(
                    "Action drop-partition must specify partitions needed to be dropped.\n"
                            + "Run drop-partition --help for help.");
            System.err.println(
                    "Action drop-partition must specify partitions needed to be dropped.\n"
                            + "Run drop-partition --help for help.");

            return Optional.empty();
        }

        List<Map<String, String>> partitions = getPartitions(params);
        if (partitions == null) {
            return Optional.empty();
        }

        return Optional.of(new DropPartitionAction(tablePath, partitions));
    }

    private static void printHelp() {
        System.out.println(
                "Action \"drop-partition\" drops data of specified partitions for a table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  drop-partition --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> --partition <partition-name> [--partition <partition-name> ...]");
        System.out.println(
                "  drop-partition --path <table-path> --partition <partition-name> [--partition <partition-name> ...]");
        System.out.println();

        System.out.println("Partition name syntax:");
        System.out.println("  key1=value1,key2=value2,...");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  drop-partition --warehouse hdfs:///path/to/warehouse --database test_db --table test_table --partition dt=20221126,hh=08");
        System.out.println(
                "  drop-partition --path hdfs:///path/to/warehouse/test_db.db/test_table --partition dt=20221126,hh=08 --partition dt=20221127,hh=09");
    }

    @Override
    public void run() throws Exception {
        commit.commit(new ArrayList<>());
    }
}
