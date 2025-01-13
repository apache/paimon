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

package org.apache.paimon.flink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.TableInfo;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.gateway.workflow.scheduler.EmbeddedQuartzScheduler;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.table.shaded.org.quartz.JobKey;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.CommonCatalogOptions.TABLE_CATALOG_STORE_KIND;
import static org.apache.flink.table.factories.FactoryUtil.WORKFLOW_SCHEDULER_TYPE;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.fetchAllResults;
import static org.apache.flink.test.util.TestUtils.waitUntilAllTasksAreRunning;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test the support of Materialized Table. */
public class MaterializedTableITCase {
    private static final String FILE_CATALOG_STORE = "file_store";
    private static final String TEST_CATALOG_PREFIX = "test_catalog";
    protected static final String TEST_DEFAULT_DATABASE = "default";

    private static final AtomicLong COUNTER = new AtomicLong(0);

    @RegisterExtension
    @Order(1)
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .build());

    @RegisterExtension
    @Order(2)
    protected static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    @RegisterExtension
    @Order(3)
    protected static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(
                    () ->
                            Executors.newCachedThreadPool(
                                    new ExecutorThreadFactory(
                                            "SqlGatewayService Test Pool",
                                            IgnoreExceptionHandler.INSTANCE)));

    @RegisterExtension
    @Order(4)
    protected static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    protected static SqlGatewayServiceImpl service;
    private static SessionEnvironment defaultSessionEnvironment;
    private static Path baseCatalogPath;

    private String paimonWarehousePath;
    protected String paimonCatalogName;

    protected SessionHandle sessionHandle;

    protected RestClusterClient<?> restClusterClient;

    @BeforeAll
    static void setUp(@TempDir Path temporaryFolder) throws Exception {
        service = (SqlGatewayServiceImpl) SQL_GATEWAY_SERVICE_EXTENSION.getService();

        // initialize file catalog store path
        Path fileCatalogStore = temporaryFolder.resolve(FILE_CATALOG_STORE);
        Files.createDirectory(fileCatalogStore);
        Map<String, String> catalogStoreOptions = new HashMap<>();
        catalogStoreOptions.put(TABLE_CATALOG_STORE_KIND.key(), "file");
        catalogStoreOptions.put("table.catalog-store.file.path", fileCatalogStore.toString());

        // initialize catalog base path
        baseCatalogPath = temporaryFolder.resolve(TEST_CATALOG_PREFIX);
        Files.createDirectory(baseCatalogPath);

        // workflow scheduler config
        Map<String, String> workflowSchedulerConfig = new HashMap<>();
        workflowSchedulerConfig.put(WORKFLOW_SCHEDULER_TYPE.key(), "embedded");
        workflowSchedulerConfig.put(
                "sql-gateway.endpoint.rest.address",
                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress());
        workflowSchedulerConfig.put(
                "sql-gateway.endpoint.rest.port",
                String.valueOf(SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort()));

        // Session conf for testing purpose
        Map<String, String> testConf = new HashMap<>();
        testConf.put("k1", "v1");
        testConf.put("k2", "v2");

        defaultSessionEnvironment =
                SessionEnvironment.newBuilder()
                        .addSessionConfig(catalogStoreOptions)
                        .addSessionConfig(workflowSchedulerConfig)
                        .addSessionConfig(testConf)
                        .setSessionEndpointVersion(new EndpointVersion() {})
                        .build();
    }

    @BeforeEach
    void before(@InjectClusterClient RestClusterClient<?> injectClusterClient) throws Exception {
        String randomStr = String.valueOf(COUNTER.incrementAndGet());
        // initialize warehouse path with random uuid
        Path fileCatalogPath = baseCatalogPath.resolve(randomStr);
        Files.createDirectory(fileCatalogPath);

        paimonWarehousePath = fileCatalogPath.toString();
        paimonCatalogName = TEST_CATALOG_PREFIX + randomStr;
        // initialize session handle, create paimon catalog and register it to catalog
        // store
        sessionHandle = initializeSession();

        // init rest cluster client
        restClusterClient = injectClusterClient;
    }

    @AfterEach
    void after() throws Exception {
        Set<TableInfo> tableInfos =
                service.listTables(
                        sessionHandle,
                        paimonCatalogName,
                        TEST_DEFAULT_DATABASE,
                        Collections.singleton(CatalogBaseTable.TableKind.TABLE));

        // drop all materialized tables
        for (TableInfo tableInfo : tableInfos) {
            ResolvedCatalogBaseTable<?> resolvedTable =
                    service.getTable(sessionHandle, tableInfo.getIdentifier());
            if (CatalogBaseTable.TableKind.MATERIALIZED_TABLE == resolvedTable.getTableKind()) {
                String dropTableDDL =
                        String.format(
                                "DROP MATERIALIZED TABLE %s",
                                tableInfo.getIdentifier().asSerializableString());
                OperationHandle dropTableHandle;
                dropTableHandle =
                        service.executeStatement(
                                sessionHandle, dropTableDDL, -1, new Configuration());
                awaitOperationTermination(service, sessionHandle, dropTableHandle);
            }
        }
    }

    @Test
    void testCreateMaterializedTableInContinuousMode() throws Exception {
        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "   'format' = 'debezium-json'\n"
                        + " )\n"
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + " FROM (\n"
                        + "    SELECT user_id, shop_id, DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM datagenSource"
                        + " ) AS tmp\n"
                        + " GROUP BY (user_id, shop_id, ds)";
        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle, materializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, materializedTableHandle);

        // validate materialized table: schema, refresh mode, refresh status, refresh handler,
        // doesn't check the data because it generates randomly.
        ResolvedCatalogMaterializedTable actualMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(
                                        paimonCatalogName, TEST_DEFAULT_DATABASE, "users_shops"));

        // Expected schema
        ResolvedSchema expectedSchema =
                ResolvedSchema.of(
                        Arrays.asList(
                                Column.physical("user_id", DataTypes.BIGINT()),
                                Column.physical("shop_id", DataTypes.BIGINT()),
                                Column.physical("ds", DataTypes.STRING()),
                                Column.physical("payed_buy_fee_sum", DataTypes.BIGINT()),
                                Column.physical("pv", DataTypes.INT().notNull())));

        assertThat(actualMaterializedTable.getResolvedSchema()).isEqualTo(expectedSchema);
        assertThat(actualMaterializedTable.getFreshness()).isEqualTo(Duration.ofSeconds(30));
        assertThat(actualMaterializedTable.getLogicalRefreshMode())
                .isEqualTo(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC);
        assertThat(actualMaterializedTable.getRefreshMode())
                .isEqualTo(CatalogMaterializedTable.RefreshMode.CONTINUOUS);
        assertThat(actualMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);
        assertThat(actualMaterializedTable.getRefreshHandlerDescription()).isNotEmpty();
        assertThat(actualMaterializedTable.getSerializedRefreshHandler()).isNotEmpty();

        ContinuousRefreshHandler activeRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        actualMaterializedTable.getSerializedRefreshHandler(),
                        getClass().getClassLoader());

        waitUntilAllTasksAreRunning(
                restClusterClient, JobID.fromHexString(activeRefreshHandler.getJobId()));

        // verify the background job is running
        String describeJobDDL = String.format("DESCRIBE JOB '%s'", activeRefreshHandler.getJobId());
        OperationHandle describeJobHandle =
                service.executeStatement(sessionHandle, describeJobDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, describeJobHandle);
        List<RowData> jobResults = fetchAllResults(service, sessionHandle, describeJobHandle);
        assertThat(jobResults.get(0).getString(2).toString()).isEqualTo("RUNNING");

        // get checkpoint interval
        long checkpointInterval =
                getCheckpointIntervalConfig(restClusterClient, activeRefreshHandler.getJobId());
        assertThat(checkpointInterval).isEqualTo(30 * 1000);
    }

    @Test
    void testAlterMaterializedTableRefresh() throws Exception {
        long timeout = Duration.ofSeconds(20).toMillis();
        long pause = Duration.ofSeconds(2).toMillis();

        List<Row> data = new ArrayList<>();
        data.add(Row.of(1L, 1L, 1L, "2024-01-01"));
        data.add(Row.of(2L, 2L, 2L, "2024-01-02"));
        data.add(Row.of(3L, 3L, 3L, "2024-01-02"));
        createAndVerifyCreateMaterializedTableWithData(
                "my_materialized_table",
                data,
                Collections.singletonMap("ds", "yyyy-MM-dd"),
                CatalogMaterializedTable.RefreshMode.CONTINUOUS);

        // remove the last element
        data.remove(2);

        long currentTime = System.currentTimeMillis();
        String alterStatement =
                "ALTER MATERIALIZED TABLE my_materialized_table REFRESH PARTITION (ds = '2024-01-02')";
        OperationHandle alterHandle =
                service.executeStatement(sessionHandle, alterStatement, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterHandle);
        List<RowData> result = fetchAllResults(service, sessionHandle, alterHandle);
        assertThat(result.size()).isEqualTo(1);
        String jobId = result.get(0).getString(0).toString();

        // 1. verify a new job is created
        verifyRefreshJobCreated(restClusterClient, jobId, currentTime);
        // 2. verify the new job overwrite the data
        try (ExecutionInBatchModeRunner ignored = new ExecutionInBatchModeRunner()) {
            org.apache.paimon.utils.CommonTestUtils.waitUtil(
                    () ->
                            fetchTableData(sessionHandle, "SELECT * FROM my_materialized_table")
                                            .size()
                                    == data.size(),
                    Duration.ofMillis(timeout),
                    Duration.ofMillis(pause),
                    "Failed to verify the data in materialized table.");
            assertThat(
                            fetchTableData(
                                            sessionHandle,
                                            "SELECT * FROM my_materialized_table where ds = '2024-01-02'")
                                    .size())
                    .isEqualTo(1);
        }
    }

    @Test
    void testDropMaterializedTable() throws Exception {
        createAndVerifyCreateMaterializedTableWithData(
                "users_shops",
                Collections.emptyList(),
                Collections.emptyMap(),
                CatalogMaterializedTable.RefreshMode.FULL);

        JobKey jobKey =
                JobKey.jobKey(
                        "quartz_job_"
                                + ObjectIdentifier.of(
                                                paimonCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")
                                        .asSerializableString(),
                        "default_group");
        EmbeddedQuartzScheduler embeddedWorkflowScheduler =
                SQL_GATEWAY_REST_ENDPOINT_EXTENSION
                        .getSqlGatewayRestEndpoint()
                        .getQuartzScheduler();

        // verify refresh workflow is created
        assertThat(embeddedWorkflowScheduler.getQuartzScheduler().checkExists(jobKey)).isTrue();

        // Drop materialized table using drop table statement
        String dropTableUsingMaterializedTableDDL = "DROP TABLE users_shops";
        OperationHandle dropTableUsingMaterializedTableHandle =
                service.executeStatement(
                        sessionHandle, dropTableUsingMaterializedTableDDL, -1, new Configuration());

        assertThatThrownBy(
                        () ->
                                awaitOperationTermination(
                                        service,
                                        sessionHandle,
                                        dropTableUsingMaterializedTableHandle))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        String.format(
                                "Table with identifier '%s' does not exist.",
                                ObjectIdentifier.of(
                                                paimonCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")
                                        .asSummaryString()));

        // drop materialized table
        String dropMaterializedTableDDL = "DROP MATERIALIZED TABLE IF EXISTS users_shops";
        OperationHandle dropMaterializedTableHandle =
                service.executeStatement(
                        sessionHandle, dropMaterializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, dropMaterializedTableHandle);

        // verify materialized table metadata is removed
        assertThatThrownBy(
                        () ->
                                service.getTable(
                                        sessionHandle,
                                        ObjectIdentifier.of(
                                                paimonCatalogName,
                                                TEST_DEFAULT_DATABASE,
                                                "users_shops")))
                .isInstanceOf(SqlGatewayException.class)
                .hasMessageContaining("Failed to getTable.");

        // verify refresh workflow is removed
        assertThat(embeddedWorkflowScheduler.getQuartzScheduler().checkExists(jobKey)).isFalse();
    }

    private long getCheckpointIntervalConfig(RestClusterClient<?> restClusterClient, String jobId)
            throws Exception {
        CheckpointConfigInfo checkpointConfigInfo =
                sendJobRequest(
                        restClusterClient,
                        CheckpointConfigHeaders.getInstance(),
                        EmptyRequestBody.getInstance(),
                        jobId);
        return RestMapperUtils.getStrictObjectMapper()
                .readTree(
                        RestMapperUtils.getStrictObjectMapper()
                                .writeValueAsString(checkpointConfigInfo))
                .get("interval")
                .asLong();
    }

    private static <M extends JobMessageParameters, R extends RequestBody, P extends ResponseBody>
            P sendJobRequest(
                    RestClusterClient<?> restClusterClient,
                    MessageHeaders<R, P, M> headers,
                    R requestBody,
                    String jobId)
                    throws Exception {
        M jobMessageParameters = headers.getUnresolvedMessageParameters();
        jobMessageParameters.jobPathParameter.resolve(JobID.fromHexString(jobId));

        return restClusterClient
                .sendRequest(headers, jobMessageParameters, requestBody)
                .get(5, TimeUnit.SECONDS);
    }

    private SessionHandle initializeSession() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        String catalogDDL =
                String.format(
                        "CREATE CATALOG %s\n"
                                + "WITH (\n"
                                + "  'type' = 'paimon',\n"
                                + "  'warehouse' = '%s'"
                                + "  )",
                        paimonCatalogName, paimonWarehousePath);
        service.configureSession(sessionHandle, catalogDDL, -1);
        service.configureSession(
                sessionHandle, String.format("USE CATALOG %s", paimonCatalogName), -1);

        // create source table
        String dataGenSource =
                "CREATE TEMPORARY TABLE datagenSource (\n"
                        + "  order_id BIGINT,\n"
                        + "  order_number VARCHAR(20),\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  product_id BIGINT,\n"
                        + "  status BIGINT,\n"
                        + "  order_type BIGINT,\n"
                        + "  order_created_at TIMESTAMP,\n"
                        + "  payment_amount_cents BIGINT\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '10'\n"
                        + ")";
        service.configureSession(sessionHandle, dataGenSource, -1);
        return sessionHandle;
    }

    public void createAndVerifyCreateMaterializedTableWithData(
            String materializedTableName,
            List<Row> data,
            Map<String, String> partitionFormatter,
            CatalogMaterializedTable.RefreshMode refreshMode)
            throws Exception {
        long timeout = Duration.ofSeconds(20).toMillis();
        long pause = Duration.ofSeconds(2).toMillis();

        String dataId = TestValuesTableFactory.registerData(data);
        String sourceDdl =
                String.format(
                        "CREATE TEMPORARY TABLE IF NOT EXISTS my_source (\n"
                                + "  order_id BIGINT,\n"
                                + "  user_id BIGINT,\n"
                                + "  shop_id BIGINT,\n"
                                + "  order_created_at STRING\n"
                                + ")\n"
                                + "WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'true',\n"
                                + "  'data-id' = '%s'\n"
                                + ")",
                        dataId);
        OperationHandle sourceHandle =
                service.executeStatement(sessionHandle, sourceDdl, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, sourceHandle);

        String partitionFields =
                partitionFormatter != null && !partitionFormatter.isEmpty()
                        ? partitionFormatter.entrySet().stream()
                                .map(
                                        e ->
                                                String.format(
                                                        "'partition.fields.%s.date-formatter' = '%s'",
                                                        e.getKey(), e.getValue()))
                                .collect(Collectors.joining(",\n", "", ",\n"))
                        : "\n";
        String materializedTableDDL =
                String.format(
                        "CREATE MATERIALIZED TABLE %s"
                                + " PARTITIONED BY (ds)\n"
                                + " WITH(\n"
                                + "    %s"
                                + "   'format' = 'debezium-json'\n"
                                + " )\n"
                                + " FRESHNESS = INTERVAL '30' SECOND\n"
                                + " REFRESH_MODE = %s\n"
                                + " AS SELECT \n"
                                + "  user_id,\n"
                                + "  shop_id,\n"
                                + "  ds,\n"
                                + "  COUNT(order_id) AS order_cnt\n"
                                + " FROM (\n"
                                + "    SELECT user_id, shop_id, order_created_at AS ds, order_id FROM my_source"
                                + " ) AS tmp\n"
                                + " GROUP BY (user_id, shop_id, ds)",
                        materializedTableName, partitionFields, refreshMode.toString());

        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle, materializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, materializedTableHandle);
        try (ExecutionInBatchModeRunner ignore = new ExecutionInBatchModeRunner()) {
            // verify data exists in materialized table
            CommonTestUtils.waitUtil(
                    () ->
                            fetchTableData(
                                                    sessionHandle,
                                                    String.format(
                                                            "SELECT * FROM %s",
                                                            materializedTableName))
                                            .size()
                                    == data.size(),
                    Duration.ofMillis(timeout),
                    Duration.ofMillis(pause),
                    "Failed to verify the data in materialized table.");
        }
    }

    /**
     * A helper runner to wrap code with try-with-resource clause. All session execution will be
     * executed in flink batch runtime mode.
     */
    protected class ExecutionInBatchModeRunner implements AutoCloseable {
        private final String oldMode;

        ExecutionInBatchModeRunner() {
            this.oldMode = service.getSessionConfig(sessionHandle).get("execution.runtime-mode");
            service.configureSession(sessionHandle, "SET 'execution.runtime-mode' = 'batch'", -1);
        }

        @Override
        public void close() throws Exception {
            if (oldMode != null) {
                service.configureSession(
                        sessionHandle,
                        String.format("SET 'execution.runtime-mode' = '%s'", oldMode),
                        -1);
            }
        }
    }

    public List<RowData> fetchTableData(SessionHandle sessionHandle, String query) {
        Configuration configuration = new Configuration();
        OperationHandle queryHandle =
                service.executeStatement(sessionHandle, query, -1, configuration);

        return fetchAllResults(service, sessionHandle, queryHandle);
    }

    public void verifyRefreshJobCreated(
            RestClusterClient<?> restClusterClient, String jobId, long startTime) throws Exception {
        long timeout = Duration.ofSeconds(20).toMillis();
        long pause = Duration.ofSeconds(2).toMillis();

        // 1. verify a new job is created
        Optional<JobStatusMessage> job =
                restClusterClient.listJobs().get(timeout, TimeUnit.MILLISECONDS).stream()
                        .filter(j -> j.getJobId().toString().equals(jobId))
                        .findFirst();
        assertThat(job).isPresent();
        assertThat(job.get().getStartTime()).isGreaterThan(startTime);

        // 2. verify the new job is a batch job
        JobDetailsInfo jobDetailsInfo =
                restClusterClient
                        .getJobDetails(JobID.fromHexString(jobId))
                        .get(timeout, TimeUnit.MILLISECONDS);
        assertThat(jobDetailsInfo.getJobType()).isEqualTo(JobType.BATCH);

        // 3. verify the new job is finished
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        return JobStatus.FINISHED.equals(
                                restClusterClient
                                        .getJobStatus(JobID.fromHexString(jobId))
                                        .get(5, TimeUnit.SECONDS));
                    } catch (Exception ignored) {
                    }
                    return false;
                },
                Duration.ofMillis(timeout),
                Duration.ofMillis(pause),
                "Failed to verify whether the job is finished.");
    }
}
