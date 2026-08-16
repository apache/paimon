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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.PREFIX_CATALOG_PROPERTIES;
import static org.assertj.core.api.Assertions.assertThat;

/** Abstract class containing common methods for CDC source IT cases. */
public abstract class CDCSourceITCaseBase {

    private static final String CATALOG_NAME = "paimon_catalog";
    private static final long TIMEOUT_MILLIS = Duration.ofSeconds(30).toMillis();
    private static final long CHECK_INTERVAL_MILLIS = Duration.ofSeconds(1).toMillis();

    @TempDir public static java.nio.file.Path warehouseFolder;

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    protected Catalog catalog;
    protected JobClient jobClient;

    @BeforeEach
    public void initialize() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);

        FlinkCatalogFactory flinkCatalogFactory =
                FactoryUtil.discoverFactory(
                        FlinkCatalogFactory.class.getClassLoader(),
                        FlinkCatalogFactory.class,
                        FlinkCatalogFactory.IDENTIFIER);
        FlinkCatalog flinkCatalog =
                flinkCatalogFactory.createCatalog(
                        new FactoryUtil.DefaultCatalogContext(
                                "flink-catalog",
                                getCatalogOptions().toMap(),
                                null,
                                FlinkCatalog.class.getClassLoader()));
        catalog = flinkCatalog.catalog();

        tEnv.registerCatalog(CATALOG_NAME, flinkCatalog);
        tEnv.useCatalog(CATALOG_NAME);
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (jobClient != null) {
            jobClient.cancel().get(1, TimeUnit.MINUTES);
            jobClient = null;
        }
    }

    protected Options getCatalogOptions() {
        String warehouse = warehouseFolder.toFile().toString();
        Options catalogOptions = new Options();
        catalogOptions.setString("metastore", "filesystem");
        catalogOptions.setString("warehouse", warehouse);
        catalogOptions.setString("cache-enabled", "false");
        catalogOptions.setString("continuous.discovery-interval", "1s");
        return catalogOptions;
    }

    protected JobClient createAndExecutePipeline(
            StreamExecutionEnvironment env,
            Configuration additionalSinkConfig,
            Configuration additionalPipelineConfig)
            throws Exception {
        Map<String, String> sourceConfigMap = new HashMap<>();
        for (Map.Entry<String, String> catalogOption : getCatalogOptions().toMap().entrySet()) {
            sourceConfigMap.put(
                    PREFIX_CATALOG_PROPERTIES + catalogOption.getKey(), catalogOption.getValue());
        }
        sourceConfigMap.put("table.discovery-interval", "3s");
        Configuration sourceConfig = Configuration.fromMap(sourceConfigMap);
        SourceDef sourceDef =
                new SourceDef(CDCDataSourceFactory.IDENTIFIER, "Paimon Source", sourceConfig);

        Configuration sinkConfig = new Configuration();
        sinkConfig.addAll(additionalSinkConfig);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        pipelineConfig.addAll(additionalPipelineConfig);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofApplicationCluster(env);
        composer.compose(pipelineDef);
        return env.executeAsync();
    }

    protected void insertInto(TableId tableId, String... rows) throws Exception {
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s.%s VALUES " + String.join(", ", rows),
                                tableId.getSchemaName(),
                                tableId.getTableName()))
                .await();
    }

    protected void waitUtilExpectedResultInternal(TableId tableId, List<String> expectedResult)
            throws InterruptedException {
        long stopTime = System.currentTimeMillis() + TIMEOUT_MILLIS;
        List<String> actualResult;
        do {
            actualResult = getActualResult(tableId);
            Collections.sort(actualResult);
            if (expectedResult.equals(actualResult)) {
                return;
            }

            Thread.sleep(CHECK_INTERVAL_MILLIS);
        } while (System.currentTimeMillis() < stopTime);
        assertThat(actualResult).containsExactlyInAnyOrderElementsOf(expectedResult);
    }

    protected abstract List<String> getActualResult(TableId tableId);
}
