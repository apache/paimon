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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.fail;

/**
 * Base class for e2e tests.
 *
 * <p>To run e2e tests, please first build the project by <code>mvn clean package</code>.
 */
public abstract class E2eTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(E2eTestBase.class);

    private final boolean withKafka;
    private final boolean withHive;
    private final boolean withSpark;

    protected E2eTestBase() {
        this(false, false);
    }

    protected E2eTestBase(boolean withKafka, boolean withHive) {
        this(withKafka, withHive, false);
    }

    protected E2eTestBase(boolean withKafka, boolean withHive, boolean withSpark) {
        this.withKafka = withKafka;
        this.withHive = withHive;
        this.withSpark = withSpark;
    }

    protected static final String TEST_DATA_DIR = "/test-data";
    protected static final String HDFS_ROOT = "hdfs://namenode:8020";

    private static final String PRINT_SINK_IDENTIFIER = "paimon-e2e-result";
    private static final int CHECK_RESULT_INTERVAL_MS = 1000;
    private static final int CHECK_RESULT_RETRIES = 60;
    private final List<String> currentResults = new ArrayList<>();

    protected Network network;
    protected ComposeContainer environment;
    protected ContainerState jobManager;

    @BeforeEach
    public void before() throws Exception {
        List<String> services = new ArrayList<>();
        services.add("jobmanager");
        services.add("taskmanager");

        network = Network.newNetwork();
        LOG.info("Network {} created", network.getId());
        environment =
                new ComposeContainer(
                                new File(
                                        E2eTestBase.class
                                                .getClassLoader()
                                                .getResource("docker-compose.yaml")
                                                .toURI()))
                        .withEnv("NETWORK_ID", ((Network.NetworkImpl) network).getName())
                        .withLogConsumer("jobmanager-1", new LogConsumer(LOG))
                        .withLogConsumer("taskmanager-1", new LogConsumer(LOG))
                        .withStartupTimeout(Duration.ofMinutes(3))
                        .withLocalCompose(true);
        if (withKafka) {
            List<String> kafkaServices = Arrays.asList("zookeeper", "kafka");
            services.addAll(kafkaServices);
            for (String s : kafkaServices) {
                environment.withLogConsumer(s + "-1", new Slf4jLogConsumer(LOG));
            }
            environment.waitingFor("kafka-1", buildWaitStrategy(".*Recorded new controller.*", 2));
        }
        if (withHive) {
            List<String> hiveServices =
                    Arrays.asList(
                            "namenode",
                            "datanode",
                            "hive-server",
                            "hive-metastore",
                            "hive-metastore-postgresql");
            services.addAll(hiveServices);
            for (String s : hiveServices) {
                environment.withLogConsumer(s + "-1", new Slf4jLogConsumer(LOG));
            }
            environment.waitingFor(
                    "hive-server-1", buildWaitStrategy(".*Starting HiveServer2.*", 1));
        }
        if (withSpark) {
            List<String> sparkServices = Arrays.asList("spark-master", "spark-worker");
            services.addAll(sparkServices);
            for (String s : sparkServices) {
                environment.withLogConsumer(s + "-1", new Slf4jLogConsumer(LOG));
            }
            environment.waitingFor(
                    "spark-master-1",
                    buildWaitStrategy(
                            ".*Master: I have been elected leader! New state: ALIVE.*", 1));
        }
        environment.withServices(services.toArray(new String[0])).withLocalCompose(true);

        environment.waitingFor("jobmanager-1", buildWaitStrategy(".*Registering TaskManager.*", 1));
        environment.waitingFor(
                "taskmanager-1",
                buildWaitStrategy(".*Successful registration at resource manager.*", 1));
        environment.start();

        jobManager = environment.getContainerByServiceName("jobmanager-1").get();
        jobManager.execInContainer("chown", "-R", "flink:flink", TEST_DATA_DIR);
    }

    private WaitStrategy buildWaitStrategy(String regex, int times) {
        // Increase timeout from 60s (default value) to 180s
        return Wait.forLogMessage(regex, times).withStartupTimeout(Duration.ofSeconds(180));
    }

    @AfterEach
    public void after() {
        if (environment != null) {
            environment.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    protected void writeSharedFile(String filename, String content) throws Exception {
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

    protected void createKafkaTopic(String topicName, int partitionNum)
            throws IOException, InterruptedException {
        assert withKafka;
        ContainerState kafka = environment.getContainerByServiceName("kafka-1").get();
        kafka.execInContainer(
                "bash",
                "-c",
                String.format(
                        "kafka-topics --create --bootstrap-server kafka:29092 --replication-factor 1 --partitions %d --topic %s",
                        partitionNum, topicName));
    }

    protected void sendKafkaMessage(String filename, String content, String topicName)
            throws IOException, InterruptedException {
        assert withKafka;
        ContainerState kafka = environment.getContainerByServiceName("kafka-1").get();
        String uuid = topicName.substring("ts-topic-".length() + 1);
        String tmpDir = "/tmp" + TEST_DATA_DIR + "/" + uuid;
        kafka.execInContainer("mkdir", "-p", tmpDir);
        kafka.execInContainer("touch", tmpDir + "/" + filename);
        kafka.execInContainer(
                "bash",
                "-c",
                String.format("cat > %s/%s <<EOF\n%s\nEOF\n", tmpDir, filename, content));
        kafka.execInContainer(
                "bash",
                "-c",
                String.format(
                        "kafka-console-producer --bootstrap-server kafka:29092 --topic %s < %s/%s",
                        topicName, tmpDir, filename));
    }

    private static final String PAIMON_HIVE_CONNECTOR_JAR_NAME = "paimon-hive-connector.jar";

    protected void setupHiveConnector() throws Exception {
        getHive()
                .execInContainer(
                        "/bin/bash",
                        "-c",
                        "mkdir /opt/hive/auxlib && cp /jars/"
                                + PAIMON_HIVE_CONNECTOR_JAR_NAME
                                + " /opt/hive/auxlib");
    }

    protected ContainerState getHive() {
        return environment.getContainerByServiceName("hive-server-1").get();
    }

    private static final Pattern JOB_ID_PATTERN =
            Pattern.compile(
                    "SQL update statement has been successfully submitted to the cluster:\\s+Job ID: (\\S+)");

    protected String runStreamingSql(String sql, String... ddls) throws Exception {
        return runStreamingSql(String.join("\n", ddls) + "\n" + sql);
    }

    protected String runStreamingSql(String sql) throws Exception {
        sql = "SET 'execution.checkpointing.interval' = '1s';\n" + "\n" + sql;
        return runSql(sql);
    }

    protected String runBatchSql(String sql, String... ddls) throws Exception {
        return runBatchSql(String.join("\n", ddls) + "\n" + sql);
    }

    protected String runBatchSql(String sql) throws Exception {
        sql =
                "SET 'execution.runtime-mode' = 'batch';\n"
                        + "SET 'table.dml-sync' = 'true';\n"
                        + "\n"
                        + sql;
        return runSql(sql);
    }

    private String runSql(String sql) throws Exception {
        String fileName = UUID.randomUUID() + ".sql";
        writeSharedFile(fileName, sql);
        Container.ExecResult execResult =
                jobManager.execInContainer(
                        "su",
                        "flink",
                        "-c",
                        "bin/sql-client.sh -f " + TEST_DATA_DIR + "/" + fileName);
        LOG.info(execResult.getStdout());
        LOG.info(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the SQL job.");
        }

        Matcher matcher = JOB_ID_PATTERN.matcher(execResult.getStdout());
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null;
        }
    }

    protected String createResultSink(String sinkName, String schema) {
        String testDataSinkDdl =
                "CREATE TEMPORARY TABLE %s ( %s ) WITH (\n"
                        + "    'connector' = 'print',\n"
                        + "    'print-identifier' = '%s'\n"
                        + ");";
        return String.format(testDataSinkDdl, sinkName, schema, PRINT_SINK_IDENTIFIER);
    }

    private List<String> getCurrentResults() {
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
