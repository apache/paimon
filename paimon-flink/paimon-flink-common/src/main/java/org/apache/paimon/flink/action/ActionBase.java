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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.UUID;

/** Abstract base of {@link Action} for table. */
public abstract class ActionBase implements Action {

    private final Options catalogOptions;

    protected final Catalog catalog;
    protected final FlinkCatalog flinkCatalog;
    protected final String catalogName = "paimon-" + UUID.randomUUID();

    public ActionBase(String warehouse, Map<String, String> catalogConfig) {
        catalogOptions = Options.fromMap(catalogConfig);
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);

        catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        flinkCatalog = FlinkCatalogFactory.createCatalog(catalogName, catalog);
    }

    protected void execute(StreamExecutionEnvironment env, String defaultName) throws Exception {
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        String name = conf.getOptional(PipelineOptions.NAME).orElse(defaultName);
        env.execute(name);
    }

    protected Catalog.Loader catalogLoader() {
        // to make the action workflow serializable
        Options catalogOptions = this.catalogOptions;
        return () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
    }
}
