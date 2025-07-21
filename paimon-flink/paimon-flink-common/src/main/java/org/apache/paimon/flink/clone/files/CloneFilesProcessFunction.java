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

package org.apache.paimon.flink.clone.files;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.flink.FlinkCatalogFactory.createPaimonCatalog;
import static org.apache.paimon.flink.clone.CloneHiveTableUtils.getRootHiveCatalog;

/** Abstract function for copying tables. */
public abstract class CloneFilesProcessFunction<I, O> extends ProcessFunction<I, O> {

    protected static final Logger LOG = LoggerFactory.getLogger(CloneFilesProcessFunction.class);

    protected final Map<String, String> sourceCatalogConfig;
    protected final Map<String, String> targetCatalogConfig;

    protected transient HiveCatalog hiveCatalog;
    protected transient Catalog targetCatalog;

    protected transient Map<Identifier, Table> tableCache;
    protected transient DataFileMetaSerializer dataFileSerializer;

    public CloneFilesProcessFunction(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.18-.
     */
    public void open(OpenContext openContext) throws Exception {
        open(new Configuration());
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public void open(Configuration conf) throws Exception {
        this.hiveCatalog =
                getRootHiveCatalog(createPaimonCatalog(Options.fromMap(sourceCatalogConfig)));
        this.targetCatalog = createPaimonCatalog(Options.fromMap(targetCatalogConfig));
        this.dataFileSerializer = new DataFileMetaSerializer();
        this.tableCache = new HashMap<>();
    }

    protected Table getTable(Identifier identifier) {
        return tableCache.computeIfAbsent(
                identifier,
                k -> {
                    try {
                        return targetCatalog.getTable(k);
                    } catch (Catalog.TableNotExistException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (hiveCatalog != null) {
            this.hiveCatalog.close();
        }
        if (targetCatalog != null) {
            this.targetCatalog.close();
        }
    }
}
