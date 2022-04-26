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

package org.apache.flink.table.store.tests;

import org.apache.flink.table.store.tests.utils.TestUtils;

import com.github.dockerjava.api.model.Volume;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Base class for e2e tests.
 *
 * <p>To run e2e tests, please first build the project by <code>mvn clean package</code>.
 */
public abstract class E2eTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(E2eTestBase.class);

    // ------------------------------------------------------------------------------------------
    // Flink Variables
    // ------------------------------------------------------------------------------------------
    private static final String FLINK_IMAGE_TAG;

    static {
        Properties properties = new Properties();
        try {
            properties.load(
                    E2eTestBase.class.getClassLoader().getResourceAsStream("project.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        FLINK_IMAGE_TAG = "amd64/flink:" + properties.getProperty("flink.version");
    }

    private static final String INTER_CONTAINER_JM_ALIAS = "jobmanager";
    private static final String INTER_CONTAINER_TM_ALIAS = "taskmanager";
    private static final int JOB_MANAGER_REST_PORT = 8081;
    private static final String FLINK_PROPERTIES =
            String.join(
                    "\n",
                    Arrays.asList(
                            "jobmanager.rpc.address: jobmanager",
                            "taskmanager.numberOfTaskSlots: 9",
                            "parallelism.default: 3",
                            "sql-client.execution.result-mode: TABLEAU"));

    // ------------------------------------------------------------------------------------------
    // Additional Jars
    // ------------------------------------------------------------------------------------------
    private static final String TABLE_STORE_JAR_NAME = "flink-table-store.jar";
    private static final String BUNDLED_HADOOP_JAR_NAME = "bundled-hadoop.jar";

    protected static final String TEST_DATA_DIR = "/opt/flink/test-data";

    @ClassRule public static final Network NETWORK = Network.newNetwork();
    private GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;

    private static final String PRINT_SINK_IDENTIFIER = "table-store-e2e-result";
    private static final int CHECK_RESULT_INTERVAL_MS = 1000;
    private static final int CHECK_RESULT_RETRIES = 60;
    private final List<String> currentResults = new ArrayList<>();

    @BeforeEach
    public void before() throws Exception {
        Volume volume = new Volume(TEST_DATA_DIR);
        jobManager =
                new GenericContainer<>(FLINK_IMAGE_TAG)
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                        .withExposedPorts(JOB_MANAGER_REST_PORT)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .withLogConsumer(new LogConsumer(LOG))
                        .withCreateContainerCmdModifier(
                                cmd ->
                                        cmd.withName("jobmanager-" + UUID.randomUUID().toString())
                                                .withVolumes(volume));
        jobManager.start();
        jobManager.execInContainer("chown", "-R", "flink:flink", TEST_DATA_DIR);
        LOG.info("Job manager started.");

        taskManager =
                new GenericContainer<>(FLINK_IMAGE_TAG)
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_TM_ALIAS)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .withVolumesFrom(jobManager, BindMode.READ_WRITE)
                        .dependsOn(jobManager)
                        .withLogConsumer(new LogConsumer(LOG));
        taskManager.start();
        LOG.info("Task manager started.");

        copyResource(TABLE_STORE_JAR_NAME);
        copyResource(BUNDLED_HADOOP_JAR_NAME);
    }

    @AfterEach
    public void after() throws Exception {
        taskManager.stop();
        jobManager.stop();
        LOG.info("Job manager and task manager stopped.");
    }

    private void copyResource(String resourceName) throws Exception {
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource(resourceName).toString()),
                TEST_DATA_DIR + "/" + resourceName);
    }

    protected void writeTestData(String filename, String content) throws Exception {
        if (content.length() == 0 || content.charAt(content.length() - 1) != '\n') {
            content += "\n";
        }
        LOG.info("Writing file {} with content\n{}", filename, content);

        if (filename.contains("/")) {
            String[] split = filename.split("/");
            jobManager.execInContainer(
                    "su",
                    "flink",
                    "-c",
                    "mkdir -p "
                            + TEST_DATA_DIR
                            + "/"
                            + String.join("/", Arrays.copyOfRange(split, 0, split.length - 1)));
        }
        jobManager.execInContainer(
                "su",
                "flink",
                "-c",
                "cat >" + TEST_DATA_DIR + "/" + filename + " <<EOF\n" + content + "EOF\n");
    }

    protected void runSql(String sql) throws Exception {
        String fileName = UUID.randomUUID().toString() + ".sql";
        writeTestData(fileName, sql);
        Container.ExecResult execResult =
                jobManager.execInContainer(
                        "su",
                        "flink",
                        "-c",
                        "bin/sql-client.sh -f "
                                + TEST_DATA_DIR
                                + "/"
                                + fileName
                                // run with table store jar
                                + " --jar "
                                + TEST_DATA_DIR
                                + "/"
                                + TABLE_STORE_JAR_NAME
                                // run with bundled hadoop jar
                                + " --jar "
                                + TEST_DATA_DIR
                                + "/"
                                + BUNDLED_HADOOP_JAR_NAME);
        LOG.info(execResult.getStdout());
        LOG.info(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the SQL job.");
        }
    }

    protected String createResultSink(String sinkName, String schema) {
        String testDataSinkDdl =
                "CREATE TABLE %s ( %s ) WITH (\n"
                        + "    'connector' = 'print',\n"
                        + "    'print-identifier' = '%s'\n"
                        + ");";
        return String.format(testDataSinkDdl, sinkName, schema, PRINT_SINK_IDENTIFIER);
    }

    protected List<String> getCurrentResults() {
        synchronized (currentResults) {
            return new ArrayList<>(currentResults);
        }
    }

    protected void clearCurrentResults() {
        synchronized (currentResults) {
            currentResults.clear();
        }
    }

    protected void checkResult(String... expected) throws Exception {
        Map<String, Integer> expectedMap = new HashMap<>();
        for (String s : expected) {
            expectedMap.compute(s, (k, v) -> (v == null ? 0 : v) + 1);
        }

        Map<String, Integer> actual = null;
        for (int tries = 1; tries <= CHECK_RESULT_RETRIES; tries++) {
            actual = new HashMap<>();
            for (String s : getCurrentResults()) {
                String key = s.substring(s.indexOf("[") + 1, s.length() - 1);
                int delta = s.startsWith("+") ? 1 : -1;
                actual.compute(key, (k, v) -> (v == null ? 0 : v) + delta);
            }
            actual.entrySet().removeIf(e -> e.getValue() == 0);
            if (actual.equals(expectedMap)) {
                return;
            }
            Thread.sleep(CHECK_RESULT_INTERVAL_MS);
        }

        fail(
                "Result is still unexpected after "
                        + CHECK_RESULT_RETRIES
                        + " retries.\nExpected: "
                        + expectedMap
                        + "\nActual: "
                        + actual);
    }

    protected void checkResult(Function<String, String> pkExtractor, String... expected)
            throws Exception {
        Map<String, String> expectedMap = new HashMap<>();
        for (String s : expected) {
            expectedMap.put(pkExtractor.apply(s), s);
        }

        Map<String, String> actual = null;
        for (int tries = 1; tries <= CHECK_RESULT_RETRIES; tries++) {
            actual = new HashMap<>();
            for (String s : getCurrentResults()) {
                String record = s.substring(s.indexOf("[") + 1, s.length() - 1);
                String pk = pkExtractor.apply(record);
                boolean insert = s.startsWith("+");
                if (insert) {
                    actual.put(pk, record);
                } else {
                    actual.remove(pk);
                }
            }
            if (actual.equals(expectedMap)) {
                return;
            }
            Thread.sleep(CHECK_RESULT_INTERVAL_MS);
        }

        fail(
                "Result is still unexpected after "
                        + CHECK_RESULT_RETRIES
                        + " retries.\nExpected: "
                        + expectedMap
                        + "\nActual: "
                        + actual);
    }

    private class LogConsumer extends Slf4jLogConsumer {

        public LogConsumer(Logger logger) {
            super(logger);
        }

        @Override
        public void accept(OutputFrame outputFrame) {
            super.accept(outputFrame);

            OutputFrame.OutputType outputType = outputFrame.getType();
            String utf8String = outputFrame.getUtf8String();
            utf8String = utf8String.replaceAll("((\\r?\\n)|(\\r))$", "");

            if (outputType == OutputFrame.OutputType.STDOUT
                    && utf8String.contains(PRINT_SINK_IDENTIFIER)) {
                synchronized (currentResults) {
                    currentResults.add(utf8String.substring(utf8String.indexOf(">") + 1).trim());
                }
            }
        }
    }
}
