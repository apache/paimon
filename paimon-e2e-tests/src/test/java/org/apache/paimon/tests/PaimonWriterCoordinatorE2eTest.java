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

package org.apache.paimon.tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for committing an append table through Paimon writer coordinator. */
@Timeout(300)
public class PaimonWriterCoordinatorE2eTest extends E2eTestBase {

    private static final long WAIT_TIMEOUT_MS = 120_000L;
    private static final Pattern VERTEX_PATTERN =
            Pattern.compile(
                    "\\\"id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"[^{}]*"
                            + "\\\"name\\\"\\s*:\\s*\\\"[^\\\"]*"
                            + "Writer\\(write-only\\)\\s*:\\s*pip30_sink[^\\\"]*\\\"");
    private static final Pattern INTEGER_PATTERN = Pattern.compile("(\\d+)");

    public PaimonWriterCoordinatorE2eTest() {
        super(false, false, false, 2);
    }

    @Override
    protected String flinkEnvFile() {
        return "flink-pwc.env";
    }

    @Test
    public void testCheckpointCommitWithWriterCoordinator() throws Exception {
        TestContext context = createContext();
        writeRecords(context.inputDirectory, 0, 20);

        String jobId = submit(context);
        waitForJobStatus(jobId, "RUNNING");
        waitForWriterSubtasks(jobId);
        waitForRecords();
        triggerAndWaitForCompletedCheckpoint(jobId);

        assertThat(rest("GET", "/jobs/" + jobId + "/plan", null))
                .doesNotContain("Committer", "Compact Coordinator", "Compact Worker");
        waitUntil(
                () -> jobManager.getLogs().contains("Paimon writer coordinator starting"),
                "PWC did not start.");

        cancel(jobId);
        assertTable(context, 0, 20);
    }

    @Test
    public void testPartialCheckpointAbortIsRecoveredByNextCheckpoint() throws Exception {
        TestContext context = createContext();
        writeRecords(context.inputDirectory, 0, 20);

        // 先启动流作业并完成一次正常 checkpoint，确保 coordinator 已经完成一次提交。
        String jobId = submit(context);
        waitForJobStatus(jobId, "RUNNING");
        waitForWriterSubtasks(jobId);
        waitForRecords();
        triggerAndWaitForCompletedCheckpoint(jobId);

        // 记录 writer 两个 subtask 的 attempt 和所在 TM，后面用来确认失败 checkpoint 不会重启任务。
        String writerVertexId = findWriterVertexId(jobId);
        Map<Integer, SubtaskAttempt> before = waitForWriterSubtasks(jobId);
        assertThat(before).hasSize(2);
        assertThat(before.get(0).host).isNotEqualTo(before.get(1).host);

        // 写入第二批数据，然后暂停其中一个 writer 所在 TM，让 checkpoint 进入部分完成状态。
        writeRecords(context.inputDirectory, 20, 20);
        waitForRecords();
        ContainerState pausedTaskManager = findTaskManager(before.get(1));
        int failedBefore = checkpointCount(jobId, "failed");
        boolean paused = false;
        try {
            pausedTaskManager
                    .getDockerClient()
                    .pauseContainerCmd(pausedTaskManager.getContainerId())
                    .exec();
            paused = true;

            triggerCheckpoint(jobId);
            waitForPartialCheckpoint(jobId);
            waitForCheckpointCount(jobId, "failed", failedBefore + 1);
        } finally {
            // 恢复被暂停的 TM，使后续 checkpoint 可以重新收齐所有 writer 的 committable。
            if (paused) {
                pausedTaskManager
                        .getDockerClient()
                        .unpauseContainerCmd(pausedTaskManager.getContainerId())
                        .exec();
            }
        }

        // 再触发一次成功 checkpoint，验证前一次 abort 的部分提交信息能被下一次 checkpoint 恢复。
        waitForJobStatus(jobId, "RUNNING");
        triggerAndWaitForDataCommitted(jobId, context);
        Map<Integer, SubtaskAttempt> after = getSubtaskAttempts(jobId, writerVertexId);
        assertThat(after.get(0).attempt).isEqualTo(before.get(0).attempt);
        assertThat(after.get(1).attempt).isEqualTo(before.get(1).attempt);

        // 取消流作业释放 slot 后，用 batch query 校验两批数据最终一致。
        cancel(jobId);
        assertTable(context, 0, 40);
    }

    @Test
    public void testTaskManagerFailureRestoresOnlyAffectedRegion() throws Exception {
        TestContext context = createContext();
        writeRecords(context.inputDirectory, 0, 40);

        String jobId = submit(context);
        waitForJobStatus(jobId, "RUNNING");
        waitForWriterSubtasks(jobId);
        waitForRecords();
        triggerAndWaitForDataCommitted(jobId, context);

        String writerVertexId = findWriterVertexId(jobId);
        assertSourceAndWriterAreChained(jobId);
        Map<Integer, SubtaskAttempt> before = waitForWriterSubtasks(jobId);
        assertThat(before).hasSize(2);
        assertThat(before.get(0).host).isNotEqualTo(before.get(1).host);

        ContainerState failedTaskManager = findTaskManager(before.get(0));
        failedTaskManager
                .getDockerClient()
                .restartContainerCmd(failedTaskManager.getContainerId())
                .withTimeout(10)
                .exec();

        waitUntil(
                () -> {
                    Map<Integer, SubtaskAttempt> attempts =
                            getSubtaskAttempts(jobId, writerVertexId);
                    return attempts.size() == 2
                            && attempts.values().stream()
                                    .allMatch(attempt -> "RUNNING".equals(attempt.status))
                            && attempts.get(0).attempt > before.get(0).attempt
                            && "RUNNING".equals(jobStatus(jobId));
                },
                "The affected writer region did not recover.");

        Map<Integer, SubtaskAttempt> after = getSubtaskAttempts(jobId, writerVertexId);
        assertThat(after.get(0).attempt).isGreaterThan(before.get(0).attempt);
        assertThat(after.get(1).attempt).isEqualTo(before.get(1).attempt);

        triggerAndWaitForDataCommitted(jobId, context);
        cancel(jobId);
        assertTable(context, 0, 40);
    }

    @Test
    public void testSavepointRestoreReplaysPendingFileInfo() throws Exception {
        TestContext context = createContext();
        writeRecords(context.inputDirectory, 0, 20);

        String firstJobId = submit(context);
        waitForJobStatus(firstJobId, "RUNNING");
        waitForWriterSubtasks(firstJobId);
        waitForRecords();
        String savepoint = cancelWithSavepoint(firstJobId);

        String restoredJobId = submit(context, savepoint);
        waitForJobStatus(restoredJobId, "RUNNING");
        waitForWriterSubtasks(restoredJobId);
        writeRecords(context.inputDirectory, 20, 20);
        waitForRecords();
        triggerAndWaitForDataCommitted(restoredJobId, context);

        waitUntil(
                () ->
                        countOccurrences(jobManager.getLogs(), "Paimon writer coordinator starting")
                                >= 2,
                "PWC was not recreated after savepoint restore.");

        cancel(restoredJobId);
        assertTable(context, 0, 40);
    }

    private TestContext createContext() {
        String id = UUID.randomUUID().toString().replace("-", "");
        String inputDirectory = "pip30-input-" + id;
        String inputPath = TEST_DATA_DIR + "/" + inputDirectory;
        String warehouse = TEST_DATA_DIR + "/pip30-" + id;

        String catalogDdl =
                String.format(
                        "CREATE CATALOG pip30_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'warehouse' = '%s'\n"
                                + ");",
                        warehouse);
        String sourceDdl =
                String.format(
                        "CREATE TEMPORARY TABLE pip30_source (\n"
                                + "    sequence_id BIGINT,\n"
                                + "    payload STRING\n"
                                + ") WITH (\n"
                                + "    'connector' = 'filesystem',\n"
                                + "    'path' = '%s',\n"
                                + "    'format' = 'csv',\n"
                                + "    'source.monitor-interval' = '1 s'\n"
                                + ");",
                        inputPath);
        String tableDdl =
                "CREATE TABLE IF NOT EXISTS pip30_sink (\n"
                        + "    sequence_id BIGINT,\n"
                        + "    payload STRING\n"
                        + ") WITH (\n"
                        + "    'bucket' = '-1',\n"
                        + "    'write-only' = 'true',\n"
                        + "    'sink.committer-coordinator-operator.enabled' = 'true'\n"
                        + ");";
        return new TestContext(
                inputDirectory,
                catalogDdl,
                sourceDdl,
                tableDdl,
                warehouse + "/default.db/pip30_sink");
    }

    private String submit(TestContext context) throws Exception {
        return submit(context, null);
    }

    private String submit(TestContext context, String savepoint) throws Exception {
        String restore =
                savepoint == null
                        ? ""
                        : String.format("SET 'execution.savepoint.path' = '%s';\n", savepoint);
        return runStreamingSql(
                "INSERT INTO pip30_sink SELECT * FROM pip30_source;",
                "SET 'parallelism.default' = '2';\n"
                        + "SET 'execution.checkpointing.interval' = '1 d';\n"
                        + "SET 'execution.checkpointing.timeout' = '10 s';\n"
                        + "SET 'execution.checkpointing.tolerable-failed-checkpoints' = '1';\n"
                        + "SET 'restart-strategy' = 'fixed-delay';\n"
                        + "SET 'restart-strategy.fixed-delay.attempts' = '10';\n"
                        + "SET 'restart-strategy.fixed-delay.delay' = '1 s';\n"
                        + restore,
                context.catalogDdl,
                "USE CATALOG pip30_catalog;",
                context.tableDdl,
                context.sourceDdl);
    }

    private void writeRecords(String inputDirectory, int start, int count) throws Exception {
        StringBuilder records = new StringBuilder();
        for (int i = start; i < start + count; i++) {
            records.append(i).append(",value-").append(i).append('\n');
        }
        writeSharedFile(inputDirectory + "/" + UUID.randomUUID() + ".csv", records.toString());
    }

    private void waitForRecords() throws InterruptedException {
        Thread.sleep(2_000L);
    }

    private void assertTable(TestContext context, int start, int end) throws Exception {
        String resultDirectory = "pip30-result-" + UUID.randomUUID();
        String resultPath = TEST_DATA_DIR + "/" + resultDirectory;
        runBatchSql(
                "INSERT INTO pip30_result SELECT sequence_id, payload FROM pip30_sink;",
                context.catalogDdl,
                "USE CATALOG pip30_catalog;",
                context.tableDdl,
                String.format(
                        "CREATE TEMPORARY TABLE pip30_result (\n"
                                + "    sequence_id BIGINT,\n"
                                + "    payload STRING\n"
                                + ") WITH (\n"
                                + "    'connector' = 'filesystem',\n"
                                + "    'path' = '%s',\n"
                                + "    'format' = 'csv'\n"
                                + ");",
                        resultPath));

        Map<String, Integer> expected = new HashMap<>();
        for (int i = start; i < end; i++) {
            expected.compute(i + ",value-" + i, (k, v) -> (v == null ? 0 : v) + 1);
        }
        assertThat(readRows(resultPath)).isEqualTo(expected);
    }

    private Map<String, Integer> readRows(String path) throws Exception {
        Container.ExecResult result =
                jobManager.execInContainer(
                        "bash",
                        "-c",
                        "if [ -d "
                                + path
                                + " ]; then find "
                                + path
                                + " -type f ! -name '.*' -exec cat {} +; fi");
        assertCommandSucceeded("read result files", result);

        Map<String, Integer> rows = new HashMap<>();
        for (String row : result.getStdout().split("\\R")) {
            if (!row.trim().isEmpty()) {
                rows.compute(row.trim(), (k, v) -> (v == null ? 0 : v) + 1);
            }
        }
        return rows;
    }

    private void triggerAndWaitForDataCommitted(String jobId, TestContext context)
            throws Exception {
        waitUntil(
                () -> {
                    triggerAndWaitForCompletedCheckpoint(jobId);
                    return latestSnapshotRecordCount(context) >= 40;
                },
                "Paimon data was not committed.");
    }

    private long latestSnapshotRecordCount(TestContext context) throws Exception {
        Container.ExecResult result =
                jobManager.execInContainer(
                        "bash",
                        "-c",
                        "latest=$(ls "
                                + context.tableDirectory
                                + "/snapshot/snapshot-* 2>/dev/null | sort -V | tail -1); "
                                + "[ -n \"$latest\" ] && sed -n 's/.*\"totalRecordCount\"[ ]*:[ ]*\\([0-9][0-9]*\\).*/\\1/p' \"$latest\"");
        if (result.getExitCode() != 0) {
            return 0L;
        }
        String recordCount = result.getStdout().trim();
        if (recordCount.isEmpty()) {
            return 0L;
        }
        return Long.parseLong(recordCount);
    }

    private String findWriterVertexId(String jobId) throws Exception {
        String details = rest("GET", "/jobs/" + jobId, null);
        Matcher matcher = VERTEX_PATTERN.matcher(details);
        if (!matcher.find()) {
            throw new AssertionError("Cannot find writer vertex in job details: " + details);
        }
        return matcher.group(1);
    }

    private void assertSourceAndWriterAreChained(String jobId) throws Exception {
        String plan = rest("GET", "/jobs/" + jobId + "/plan", null);
        assertThat(plan)
                .withFailMessage(
                        "Source and writer must be chained in one parallel vertex to verify"
                                + " subtask-level region failover.%nPlan:%n%s",
                        plan)
                .contains(
                        "\"parallelism\":2",
                        "TableSourceScan(table=[[pip30_catalog, default, pip30_source]]",
                        "Writer(write-only) : pip30_sink");
    }

    private Map<Integer, SubtaskAttempt> waitForWriterSubtasks(String jobId) throws Exception {
        String writerVertexId = findWriterVertexId(jobId);
        waitUntil(
                () -> {
                    Map<Integer, SubtaskAttempt> attempts =
                            getSubtaskAttempts(jobId, writerVertexId);
                    return attempts.size() == 2
                            && attempts.values().stream()
                                    .allMatch(attempt -> "RUNNING".equals(attempt.status));
                },
                "Writer subtasks did not become available.");
        return getSubtaskAttempts(jobId, writerVertexId);
    }

    private Map<Integer, SubtaskAttempt> getSubtaskAttempts(String jobId, String vertexId)
            throws Exception {
        String details = rest("GET", "/jobs/" + jobId + "/vertices/" + vertexId, null);
        Map<Integer, SubtaskAttempt> attempts = new HashMap<>();
        String[] subtasks = details.split("\\\"subtask\\\"\\s*:");
        for (int i = 1; i < subtasks.length; i++) {
            String subtask = subtasks[i];
            Integer index = firstInteger(subtask);
            Integer attempt = integerField(subtask, "attempt");
            String status = stringField(subtask, "status");
            String host = stringField(subtask, "host");
            if (host == null) {
                // support for Flink 2.2 REST API
                host = stringField(subtask, "endpoint");
            }
            if (index != null && attempt != null && status != null && host != null) {
                attempts.put(index, new SubtaskAttempt(attempt, status, host));
            }
        }
        return attempts;
    }

    private ContainerState findTaskManager(SubtaskAttempt attempt) {
        String normalizedHost = attempt.host.replace('_', '-');
        for (int i = 1; i <= 2; i++) {
            ContainerState taskManager =
                    environment.getContainerByServiceName("taskmanager-" + i).get();
            boolean hostnameMatches =
                    normalizedHost.endsWith("-taskmanager-" + i)
                            || normalizedHost.contains("-taskmanager-" + i + ".")
                            || normalizedHost.contains("-taskmanager-" + i + ":");
            boolean ipMatches =
                    taskManager.getContainerInfo().getNetworkSettings().getNetworks().values()
                            .stream()
                            .anyMatch(
                                    network ->
                                            attempt.host.startsWith(network.getIpAddress() + ":")
                                                    || attempt.host.equals(network.getIpAddress()));
            if (hostnameMatches || ipMatches) {
                return taskManager;
            }
        }
        throw new AssertionError("Cannot map writer host to TaskManager: " + attempt.host);
    }

    private void triggerAndWaitForCompletedCheckpoint(String jobId) throws Exception {
        int completedBefore = checkpointCount(jobId, "completed");
        triggerCheckpoint(jobId);
        waitForCheckpointCount(jobId, "completed", completedBefore + 1);
    }

    private void triggerCheckpoint(String jobId) throws Exception {
        rest("POST", "/jobs/" + jobId + "/checkpoints", "{}");
    }

    private int checkpointCount(String jobId, String field) throws Exception {
        String checkpoints = rest("GET", "/jobs/" + jobId + "/checkpoints", null);
        Matcher counts = Pattern.compile("\\\"counts\\\"\\s*:\\s*\\{([^}]*)}").matcher(checkpoints);
        if (!counts.find()) {
            throw new AssertionError("Checkpoint counts are missing: " + checkpoints);
        }
        Integer value = integerField(counts.group(1), field);
        if (value == null) {
            throw new AssertionError("Checkpoint count is missing for " + field);
        }
        return value;
    }

    private void waitForCheckpointCount(String jobId, String field, int expected) throws Exception {
        waitUntil(
                () -> checkpointCount(jobId, field) >= expected,
                "Checkpoint " + field + " count did not reach " + expected + '.');
    }

    private void waitForPartialCheckpoint(String jobId) throws Exception {
        waitUntil(
                () -> {
                    String checkpoints = rest("GET", "/jobs/" + jobId + "/checkpoints", null);
                    Matcher inProgress =
                            Pattern.compile(
                                            "\\\"status\\\"\\s*:\\s*\\\"IN_PROGRESS\\\"([\\s\\S]{0,600})")
                                    .matcher(checkpoints);
                    while (inProgress.find()) {
                        Integer acknowledged =
                                integerField(inProgress.group(1), "num_acknowledged_subtasks");
                        if (acknowledged != null && acknowledged > 0) {
                            return true;
                        }
                    }
                    return false;
                },
                "Checkpoint did not receive a partial writer snapshot.");
    }

    private void waitForJobStatus(String jobId, String expected) throws Exception {
        final String[] lastStatus = new String[1];
        waitUntil(
                () -> {
                    lastStatus[0] = jobStatus(jobId);
                    return expected.equals(lastStatus[0]);
                },
                "Job "
                        + jobId
                        + " did not reach status "
                        + expected
                        + ", last status was "
                        + lastStatus[0]
                        + ". Exceptions: "
                        + rest("GET", "/jobs/" + jobId + "/exceptions", null));
    }

    private String jobStatus(String jobId) throws Exception {
        return stringField(rest("GET", "/jobs/" + jobId, null), "state");
    }

    private void cancel(String jobId) throws Exception {
        Container.ExecResult result =
                jobManager.execInContainerWithUser("flink", "bin/flink", "cancel", jobId);
        assertCommandSucceeded("cancel job", result);
        waitForJobStatus(jobId, "CANCELED");
    }

    private String cancelWithSavepoint(String jobId) throws Exception {
        String directory = TEST_DATA_DIR + "/savepoints-" + UUID.randomUUID();
        Container.ExecResult mkdir =
                jobManager.execInContainerWithUser("flink", "mkdir", "-p", directory);
        assertCommandSucceeded("create savepoint directory", mkdir);

        Container.ExecResult result =
                jobManager.execInContainerWithUser(
                        "flink", "bin/flink", "cancel", "-s", directory, jobId);
        assertCommandSucceeded("cancel job with savepoint", result);

        String output = result.getStdout() + '\n' + result.getStderr();
        Matcher path =
                Pattern.compile("Path:\\s*(\\S+)|Savepoint stored in\\s+(\\S+)\\.").matcher(output);

        if (!path.find()) {
            throw new AssertionError(
                    "Cannot find savepoint path.\nstdout:\n"
                            + result.getStdout()
                            + "\nstderr:\n"
                            + result.getStderr());
        }

        String savepointPath = path.group(1) != null ? path.group(1) : path.group(2);
        waitForJobStatus(jobId, "CANCELED");
        return savepointPath;
    }

    private void assertCommandSucceeded(String command, Container.ExecResult result) {
        assertThat(result.getExitCode())
                .withFailMessage(
                        "%s failed with exit code %s.\nstdout:\n%s\nstderr:\n%s",
                        command, result.getExitCode(), result.getStdout(), result.getStderr())
                .isZero();
    }

    private String rest(String method, String path, String body) throws Exception {
        HttpURLConnection connection =
                (HttpURLConnection) new URL(flinkRestUrl() + path).openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout(10_000);
        connection.setReadTimeout(30_000);
        if (body != null) {
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            try (OutputStream output = connection.getOutputStream()) {
                output.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }

        int responseCode = connection.getResponseCode();
        InputStream input =
                responseCode >= 200 && responseCode < 300
                        ? connection.getInputStream()
                        : connection.getErrorStream();
        String response = read(input);
        connection.disconnect();
        if (responseCode < 200 || responseCode >= 300) {
            throw new IOException(
                    "Flink REST "
                            + method
                            + ' '
                            + path
                            + " failed: "
                            + responseCode
                            + ' '
                            + response);
        }
        return response;
    }

    private static String read(InputStream input) throws IOException {
        if (input == null) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
        }
        return result.toString();
    }

    private static Integer firstInteger(String value) {
        Matcher matcher = INTEGER_PATTERN.matcher(value);
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : null;
    }

    private static Integer integerField(String value, String field) {
        Matcher matcher =
                Pattern.compile("\\\"" + Pattern.quote(field) + "\\\"\\s*:\\s*(\\d+)")
                        .matcher(value);
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : null;
    }

    private static String stringField(String value, String field) {
        Matcher matcher =
                Pattern.compile("\\\"" + Pattern.quote(field) + "\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"")
                        .matcher(value);
        return matcher.find() ? matcher.group(1) : null;
    }

    private static int countOccurrences(String value, String expected) {
        int count = 0;
        int index = 0;
        while ((index = value.indexOf(expected, index)) >= 0) {
            count++;
            index += expected.length();
        }
        return count;
    }

    private static void waitUntil(CheckedBooleanSupplier condition, String failureMessage)
            throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        Throwable lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (Throwable t) {
                lastFailure = t;
            }
            Thread.sleep(200L);
        }
        AssertionError error = new AssertionError(failureMessage);
        if (lastFailure != null) {
            error.initCause(lastFailure);
        }
        throw error;
    }

    @FunctionalInterface
    private interface CheckedBooleanSupplier {

        boolean getAsBoolean() throws Exception;
    }

    private static class TestContext {

        private final String inputDirectory;
        private final String catalogDdl;
        private final String sourceDdl;
        private final String tableDdl;
        private final String tableDirectory;

        private TestContext(
                String inputDirectory,
                String catalogDdl,
                String sourceDdl,
                String tableDdl,
                String tableDirectory) {
            this.inputDirectory = inputDirectory;
            this.catalogDdl = catalogDdl;
            this.sourceDdl = sourceDdl;
            this.tableDdl = tableDdl;
            this.tableDirectory = tableDirectory;
        }
    }

    private static class SubtaskAttempt {

        private final int attempt;
        private final String status;
        private final String host;

        private SubtaskAttempt(int attempt, String status, String host) {
            this.attempt = attempt;
            this.status = status;
            this.host = host;
        }

        @Override
        public String toString() {
            return "SubtaskAttempt{"
                    + "attempt="
                    + attempt
                    + ", status='"
                    + status
                    + '\''
                    + ", host='"
                    + host
                    + '\''
                    + '}';
        }
    }
}
