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

package org.apache.paimon.hive.mapred;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.hive.utils.HiveUtils.createFileStoreTable;

/** A Paimon table committer for adding data files to the Paimon table. */
public class PaimonOutputCommitter extends OutputCommitter {

    private static final String PRE_COMMIT = ".preCommit";

    private static final Logger LOG = LoggerFactory.getLogger(PaimonOutputCommitter.class);

    @Override
    public void setupJob(JobContext jobContext) throws IOException {}

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {}

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
        // We need to commit if this is the last phase of a MapReduce process
        return TaskType.REDUCE.equals(
                        taskAttemptContext.getTaskAttemptID().getTaskID().getTaskType())
                || taskAttemptContext.getJobConf().getNumReduceTasks() == 0;
    }

    @Override
    public void commitTask(TaskAttemptContext originalContext) throws IOException {
        TaskAttemptContext taskAttemptContext =
                TezUtil.enrichContextWithAttemptWrapper(originalContext);
        TaskAttemptID attemptID = taskAttemptContext.getTaskAttemptID();
        JobConf jobConf = taskAttemptContext.getJobConf();
        FileStoreTable table = createFileStoreTable(jobConf);

        Map<String, PaimonRecordWriter> writers =
                Optional.ofNullable(PaimonRecordWriter.getWriters(attemptID))
                        .orElseGet(
                                () -> {
                                    LOG.info(
                                            "CommitTask found no writers for output table: {}, attemptID: {}",
                                            table.name(),
                                            attemptID);
                                    return ImmutableMap.of();
                                });
        PaimonRecordWriter writer = writers.get(table.name());
        if (writer != null) {

            try (BatchTableWrite batchTableWrite = writer.batchTableWrite()) {
                List<CommitMessage> commitTables = batchTableWrite.prepareCommit();
                createPreCommitFile(
                        commitTables,
                        generatePreCommitFileLocation(
                                table.tableDataPath(),
                                attemptID.getJobID(),
                                attemptID.getTaskID().getId()),
                        table.fileIO());
                writer.close(true);
            } catch (Exception e) {
                LOG.error(
                        "CommitTask prepareCommit error for specific table: {}, attemptID: {}",
                        table.name(),
                        attemptID);
                throw new RuntimeException(e);
            }
        } else {
            LOG.info(
                    "CommitTask found no writer for specific table: {}, attemptID: {}",
                    table.name(),
                    attemptID);
        }
        PaimonRecordWriter.removeWriters(attemptID);
    }

    @Override
    public void abortTask(TaskAttemptContext originalContext) throws IOException {
        TaskAttemptContext taskAttemptContext =
                TezUtil.enrichContextWithAttemptWrapper(originalContext);
        Map<String, PaimonRecordWriter> writers =
                PaimonRecordWriter.removeWriters(taskAttemptContext.getTaskAttemptID());

        // close writer and delete files
        if (writers != null) {
            for (PaimonRecordWriter writer : writers.values()) {
                writer.close(true);
            }
        }
    }

    @Override
    public void commitJob(JobContext originalContext) throws IOException {
        JobContext jobContext = TezUtil.enrichContextWithVertexId(originalContext);
        JobConf jobConf = jobContext.getJobConf();

        long startTime = System.currentTimeMillis();
        LOG.info("CommitJob {} has started", jobContext.getJobID());
        FileStoreTable table = createFileStoreTable(jobConf);

        if (table != null) {
            BatchWriteBuilder batchWriteBuilder = table.newBatchWriteBuilder();
            List<CommitMessage> commitMessagesList =
                    getAllPreCommitMessage(table.tableDataPath(), jobContext, table.fileIO());
            try (BatchTableCommit batchTableCommit = batchWriteBuilder.newCommit()) {
                batchTableCommit.commit(commitMessagesList);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            deleteTemporaryFile(
                    jobContext,
                    generateJobLocation(table.tableDataPath(), jobContext.getJobID()),
                    table.fileIO());
        } else {
            LOG.info("CommitJob not found table, Skipping job commit.");
        }

        LOG.info(
                "Commit took {} ms for job {}",
                System.currentTimeMillis() - startTime,
                jobContext.getJobID());
    }

    @Override
    public void abortJob(JobContext originalContext, int status) throws IOException {
        JobContext jobContext = TezUtil.enrichContextWithVertexId(originalContext);
        FileStoreTable table = createFileStoreTable(jobContext.getJobConf());
        if (table != null) {

            LOG.info("AbortJob {} has started", jobContext.getJobID());
            List<CommitMessage> commitMessagesList =
                    getAllPreCommitMessage(table.tableDataPath(), jobContext, table.fileIO());
            BatchWriteBuilder batchWriteBuilder = table.newBatchWriteBuilder();
            try (BatchTableCommit batchTableCommit = batchWriteBuilder.newCommit()) {
                batchTableCommit.abort(commitMessagesList);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            deleteTemporaryFile(
                    jobContext,
                    generateJobLocation(table.tableDataPath(), jobContext.getJobID()),
                    table.fileIO());
            LOG.info("Job {} is aborted. preCommit file has deleted", jobContext.getJobID());
        }
    }

    /**
     * Delete job's temporary locations.
     *
     * @param jobContext The job context
     * @param location The locations to clean up
     */
    private void deleteTemporaryFile(JobContext jobContext, Path location, FileIO fileIO) {
        LOG.info("Deleting temporary file for job {} started", jobContext.getJobID());

        LOG.info("The deleted file is located in : {}", location);
        try {
            fileIO.delete(location, true);
        } catch (IOException e) {
            LOG.debug("Failed to delete directory {} ", location, e);
        }
        LOG.info("Deleting temporary file for job {} finished", jobContext.getJobID());
    }

    /**
     * Get the CommitMessages.
     *
     * @param location The location of the table
     * @param jobContext The job context
     * @param io The FileIO used for reading a files generated for commit
     * @return The list of the committed data files
     */
    private static List<CommitMessage> getAllPreCommitMessage(
            Path location, JobContext jobContext, FileIO io) {
        JobConf conf = jobContext.getJobConf();

        int totalCommitMessagesSize =
                conf.getNumReduceTasks() > 0 ? conf.getNumReduceTasks() : conf.getNumMapTasks();

        List<CommitMessage> commitMessagesList = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < totalCommitMessagesSize; i++) {
            Path commitFileLocation =
                    generatePreCommitFileLocation(location, jobContext.getJobID(), i);
            commitMessagesList.addAll(readPreCommitFile(commitFileLocation, io));
        }

        return commitMessagesList;
    }

    /**
     * Generates the job temp location based on the job configuration.
     *
     * @param location The location of the table
     * @param jobId The JobID for the task
     * @return The file to store the results
     */
    static Path generateJobLocation(Path location, JobID jobId) {
        return new Path(new Path(location, "temp"), jobId.toString());
    }

    /**
     * Generates preCommit file location based on the configuration and a specific task id. In order
     * to ensure the atomicity of the commit, we need to keep preCommit persistent, restore it in
     * {@link PaimonOutputCommitter#commitJob(JobContext)} to complete the final commit, and delete
     * the temporary files at the end.
     *
     * @param location The location of the table
     * @param jobId jobId
     * @param taskId taskId
     * @return The location of preCommit file path
     */
    private static Path generatePreCommitFileLocation(Path location, JobID jobId, int taskId) {
        return new Path(generateJobLocation(location, jobId), "task_" + taskId + PRE_COMMIT);
    }

    /**
     * * Create a temp preCommitFile to store {@link BatchTableWrite#prepareCommit()}'s
     * result @Param commitTables The commitMessages of the table preCommit @Param location The temp
     * file's location @Param io The FileIO of the table.
     */
    private static void createPreCommitFile(
            List<CommitMessage> commitTables, Path location, FileIO io) throws IOException {
        try (ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(io.newOutputStream(location, true))) {
            objectOutputStream.writeObject(commitTables);
        }
    }

    private static List<CommitMessage> readPreCommitFile(Path location, FileIO io) {
        try (ObjectInputStream objectInputStream =
                new ObjectInputStream(io.newInputStream(location))) {
            return (List<CommitMessage>) objectInputStream.readObject();
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(
                    String.format("Can not read or parse CommitMessage file: %s", location));
        }
    }
}
