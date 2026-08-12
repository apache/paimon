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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTaskSerializer;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactionCommitPreparation;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.EndOfScanException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.spark.utils.SparkProcedureUtils.readParallelism;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Executes planned data-evolution rewrite tasks in bounded Spark batches. */
final class DataEvolutionRewriteExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionRewriteExecutor.class);

    private DataEvolutionRewriteExecutor() {}

    static void execute(
            FileStoreTable table,
            Snapshot initialSnapshot,
            Supplier<List<DataEvolutionCompactTask>> taskPlanner,
            JavaSparkContext javaSparkContext,
            SparkSession sparkSession,
            CommitConfigurer commitConfigurer) {
        CommitMessageSerializer messageSerializer = new CommitMessageSerializer();
        String commitUser = createCommitUser(table.coreOptions().toConfiguration());
        Snapshot preparationSnapshot = initialSnapshot;
        try {
            while (true) {
                List<DataEvolutionCompactTask> compactionTasks = taskPlanner.get();
                if (compactionTasks.isEmpty()) {
                    LOG.info("Task plan is empty, no data evolution rewrite job to execute.");
                    continue;
                }

                DataEvolutionCompactTaskSerializer serializer =
                        new DataEvolutionCompactTaskSerializer();
                List<byte[]> serializedTasks = new ArrayList<>();
                try {
                    for (DataEvolutionCompactTask compactionTask : compactionTasks) {
                        serializedTasks.add(serializer.serialize(compactionTask));
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Serialize data evolution rewrite task failed.", e);
                }

                int readParallelism = readParallelism(serializedTasks, sparkSession);
                JavaRDD<byte[]> commitMessageJavaRDD =
                        javaSparkContext
                                .parallelize(serializedTasks, readParallelism)
                                .mapPartitions(
                                        (FlatMapFunction<Iterator<byte[]>, byte[]>)
                                                taskIterator -> {
                                                    DataEvolutionCompactTaskSerializer ser =
                                                            new DataEvolutionCompactTaskSerializer();
                                                    List<byte[]> messagesBytes = new ArrayList<>();
                                                    CommitMessageSerializer messageSer =
                                                            new CommitMessageSerializer();
                                                    while (taskIterator.hasNext()) {
                                                        DataEvolutionCompactTask task =
                                                                ser.deserialize(
                                                                        ser.getVersion(),
                                                                        taskIterator.next());
                                                        messagesBytes.add(
                                                                messageSer.serialize(
                                                                        task.doCompact(
                                                                                table,
                                                                                commitUser)));
                                                    }
                                                    return messagesBytes.iterator();
                                                });

                List<byte[]> serializedMessages = new ArrayList<>(commitMessageJavaRDD.collect());
                try (TableCommitImpl commit = table.newCommit(commitUser)) {
                    commitConfigurer.configure(commit);
                    List<CommitMessage> messages =
                            deserializeCommitMessagesAndReleaseSerializedBytes(
                                    messageSerializer, serializedMessages);
                    messages.addAll(
                            new DataEvolutionCompactionCommitPreparation(table, preparationSnapshot)
                                    .prepare(messages));
                    commit.commit(messages);
                    Snapshot committedSnapshot =
                            table.snapshotManager()
                                    .latestSnapshotOfUser(commitUser)
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "Cannot find the committed data evolution rewrite snapshot."));
                    checkArgument(
                            committedSnapshot.id() > preparationSnapshot.id(),
                            "Committed data evolution rewrite snapshot %s must be newer than preparation snapshot %s.",
                            committedSnapshot.id(),
                            preparationSnapshot.id());
                    preparationSnapshot = committedSnapshot;
                } catch (Exception e) {
                    throw new RuntimeException("Execute data evolution rewrite failed.", e);
                }
            }
        } catch (EndOfScanException e) {
            LOG.info("Catching EndOfScanException, the data evolution rewrite job is finishing.");
        }
    }

    private static List<CommitMessage> deserializeCommitMessagesAndReleaseSerializedBytes(
            CommitMessageSerializer serializer, List<byte[]> serializedMessages)
            throws IOException {
        List<CommitMessage> messages = new ArrayList<>(serializedMessages.size());
        for (int i = 0; i < serializedMessages.size(); i++) {
            byte[] serializedMessage = serializedMessages.set(i, null);
            messages.add(serializer.deserialize(serializer.getVersion(), serializedMessage));
        }
        return messages;
    }

    @FunctionalInterface
    interface CommitConfigurer {

        void configure(TableCommitImpl commit);
    }
}
