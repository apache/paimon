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

package org.apache.paimon;

import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.types.DataTypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Generates java-nested-history.zip's table using the Java writer and compactor. Compile with the
 * paimon-core test classpath, run with an empty warehouse directory as the only argument, then zip
 * default.db/nested/ relative to that warehouse. Snapshots 1-2 are initial appends, 3 updates
 * nest.a after adding a column, and 4 compacts. Pass added-child as a second argument to generate
 * java-nested-added-child-history.zip instead: schema 0 contains only nest.a, then Java adds and
 * partially updates nest.b before compacting.
 */
public final class NestedHistoryFixtureGenerator {
    public static void main(String[] args) throws Exception {
        boolean addedChild = args.length > 1 && "added-child".equals(args[1]);
        Identifier identifier = Identifier.create("default", "nested");
        try (Catalog catalog =
                CatalogFactory.createCatalog(CatalogContext.create(new Path(args[0])))) {
            catalog.createDatabase("default", false);
            Schema schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("age", DataTypes.INT())
                            .column(
                                    "nest",
                                    addedChild
                                            ? DataTypes.ROW(
                                                    DataTypes.FIELD(0, "a", DataTypes.INT()))
                                            : DataTypes.ROW(
                                                    DataTypes.FIELD(0, "a", DataTypes.INT()),
                                                    DataTypes.FIELD(1, "b", DataTypes.STRING())))
                            .option("row-tracking.enabled", "true")
                            .option("data-evolution.enabled", "true")
                            .option("data-evolution.nested-field.enabled", "true")
                            .option("compaction.min.file-num", "2")
                            .option("write-only", "true")
                            .option("write.native.enabled", "false")
                            .option("commit.native.enabled", "false")
                            .option("scan.native-plan.enabled", "false")
                            .option("read.native.enabled", "false")
                            .build();
            catalog.createTable(identifier, schema, false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            for (int start : new int[] {1, 3}) {
                BatchWriteBuilder builder = table.newBatchWriteBuilder();
                try (BatchTableWrite writer = builder.newWrite()) {
                    for (int id = start; id < start + 2; id++) {
                        writer.write(
                                GenericRow.of(
                                        id,
                                        id * 10,
                                        addedChild
                                                ? GenericRow.of(id * 100)
                                                : GenericRow.of(
                                                        id * 100,
                                                        BinaryString.fromString("row-" + id))));
                    }
                    commit(builder, writer.prepareCommit());
                }
            }
            catalog.alterTable(
                    identifier,
                    addedChild
                            ? SchemaChange.addColumn(
                                    new String[] {"nest", "b"}, DataTypes.STRING(), null, null)
                            : SchemaChange.addColumn("extra", DataTypes.INT()),
                    false);
            table = (FileStoreTable) catalog.getTable(identifier);
            String writePath = addedChild ? "nest.b" : "nest.a";
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite writer =
                    builder.newWrite()
                            .withWriteType(
                                    table.rowType()
                                            .projectByPaths(
                                                    Collections.singletonList(writePath)))) {
                for (int id = 1; id <= 2; id++) {
                    writer.write(
                            GenericRow.of(
                                    GenericRow.of(
                                            addedChild
                                                    ? BinaryString.fromString("preserve-" + id)
                                                    : id * 1000)));
                }
                List<CommitMessage> messages = writer.prepareCommit();
                for (CommitMessage message : messages) {
                    List<DataFileMeta> files =
                            ((CommitMessageImpl) message).newFilesIncrement().newFiles();
                    for (int i = 0; i < files.size(); i++) {
                        if (!Collections.singletonList(writePath)
                                .equals(files.get(i).writeCols())) {
                            throw new IllegalStateException("Expected a Java dotted write column");
                        }
                        files.set(i, files.get(i).assignFirstRowId(0));
                    }
                }
                commit(builder, messages);
            }
            DataEvolutionCompactCoordinator coordinator =
                    new DataEvolutionCompactCoordinator(
                            table, true, false, table.latestSnapshot().get());
            List<CommitMessage> compactMessages = new ArrayList<>();
            try {
                List<DataEvolutionCompactTask> tasks;
                while (!(tasks = coordinator.plan()).isEmpty()) {
                    for (DataEvolutionCompactTask task : tasks) {
                        compactMessages.add(task.doCompact(table, "nested-history-fixture"));
                    }
                }
            } catch (EndOfScanException ignored) {
            }
            if (compactMessages.isEmpty()) {
                throw new IllegalStateException("Expected physical compaction");
            }
            commit(table.newBatchWriteBuilder(), compactMessages);
            if (table.snapshotManager().latestSnapshot().id() != 4) {
                throw new IllegalStateException("Expected exactly four snapshots");
            }
        }
    }

    private static void commit(BatchWriteBuilder builder, List<CommitMessage> messages)
            throws Exception {
        try (BatchTableCommit commit = builder.newCommit()) {
            commit.commit(messages);
        }
    }
}
