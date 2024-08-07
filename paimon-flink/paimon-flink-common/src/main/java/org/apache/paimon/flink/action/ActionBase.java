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

package org.apache.paimon.flink.action;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;

/** Abstract base of {@link Action} for table. */
public abstract class ActionBase implements Action {

    protected final Options catalogOptions;
    protected final Catalog catalog;
    protected final FlinkCatalog flinkCatalog;
    protected final String catalogName = "paimon-" + UUID.randomUUID();

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment batchTEnv;

    public ActionBase(String warehouse, Map<String, String> catalogConfig) {
        catalogOptions = Options.fromMap(catalogConfig);
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);

        // disable cache to avoid concurrent modification exception
        if (!catalogOptions.contains(CACHE_ENABLED)) {
            catalogOptions.set(CACHE_ENABLED, false);
        }

        catalog = initPaimonCatalog();
        flinkCatalog = initFlinkCatalog();

        // use the default env if user doesn't pass one
        initFlinkEnv(StreamExecutionEnvironment.getExecutionEnvironment());
    }

    public ActionBase withStreamExecutionEnvironment(StreamExecutionEnvironment env) {
        initFlinkEnv(env);
        return this;
    }

    protected Catalog initPaimonCatalog() {
        return FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
    }

    protected FlinkCatalog initFlinkCatalog() {
        return FlinkCatalogFactory.createCatalog(catalogName, catalog, catalogOptions);
    }

    protected void initFlinkEnv(StreamExecutionEnvironment env) {
        this.env = env;
        // we enable object reuse, we copy the un-reusable object ourselves.
        this.env.getConfig().enableObjectReuse();
        batchTEnv = StreamTableEnvironment.create(this.env, EnvironmentSettings.inBatchMode());

        // register flink catalog to table environment
        batchTEnv.registerCatalog(flinkCatalog.getName(), flinkCatalog);
        batchTEnv.useCatalog(flinkCatalog.getName());
    }

    protected void execute(String defaultName) throws Exception {
        ReadableConfig conf = env.getConfiguration();
        String name = conf.getOptional(PipelineOptions.NAME).orElse(defaultName);
        env.execute(name);
    }

    protected Catalog.Loader catalogLoader() {
        // to make the action workflow serializable
        Options catalogOptions = this.catalogOptions;
        return () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
    }

    /**
     * Extract {@link LogicalType}s from Flink {@link org.apache.flink.table.types.DataType}s and
     * convert to Paimon {@link DataType}s.
     */
    protected List<DataType> toPaimonTypes(
            List<org.apache.flink.table.types.DataType> flinkDataTypes) {
        return flinkDataTypes.stream()
                .map(org.apache.flink.table.types.DataType::getLogicalType)
                .map(LogicalTypeConversion::toDataType)
                .collect(Collectors.toList());
    }

    /**
     * Check whether each {@link DataType} of actualTypes is compatible with that of expectedTypes
     * respectively.
     */
    protected boolean compatibleCheck(List<DataType> actualTypes, List<DataType> expectedTypes) {
        if (actualTypes.size() != expectedTypes.size()) {
            return false;
        }

        for (int i = 0; i < actualTypes.size(); i++) {
            if (!DataTypeCasts.supportsCompatibleCast(actualTypes.get(i), expectedTypes.get(i))) {
                return false;
            }
        }

        return true;
    }

    @VisibleForTesting
    public Map<String, String> catalogConfig() {
        return catalogOptions.toMap();
    }
}
