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

package org.apache.paimon.flink.clone.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.FlinkCatalogFactory.createPaimonCatalog;

/** List splits for table. */
public class ClonePaimonSchemaFunction
        extends ProcessFunction<Tuple2<Identifier, Identifier>, CloneSchemaInfo> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClonePaimonSchemaFunction.class);

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;
    private final String preferFileFormat;
    private final boolean cloneIfExists;

    private transient Catalog sourceCatalog;
    private transient Catalog targetCatalog;

    public ClonePaimonSchemaFunction(
            Map<String, String> sourceCatalogConfig,
            Map<String, String> targetCatalogConfig,
            String preferFileFormat,
            boolean cloneIfExists) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
        this.preferFileFormat = preferFileFormat;
        this.cloneIfExists = cloneIfExists;
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
        this.sourceCatalog = createPaimonCatalog(Options.fromMap(sourceCatalogConfig));
        this.targetCatalog = createPaimonCatalog(Options.fromMap(targetCatalogConfig));
    }

    @Override
    public void processElement(
            Tuple2<Identifier, Identifier> tuple,
            ProcessFunction<Tuple2<Identifier, Identifier>, CloneSchemaInfo>.Context context,
            Collector<CloneSchemaInfo> collector)
            throws Exception {

        // create database if not exists
        targetCatalog.createDatabase(tuple.f1.getDatabaseName(), true);

        Table sourceTable = sourceCatalog.getTable(tuple.f0);
        Schema.Builder builder = Schema.newBuilder();
        sourceTable
                .rowType()
                .getFields()
                .forEach(
                        f -> builder.column(f.name(), f.type(), f.description(), f.defaultValue()));
        builder.partitionKeys(sourceTable.partitionKeys());
        builder.primaryKey(sourceTable.primaryKeys());
        sourceTable
                .options()
                .forEach(
                        (k, v) -> {
                            if (k.equalsIgnoreCase(BUCKET.key())
                                    || k.equalsIgnoreCase(PATH.key())) {
                                return;
                            }
                            builder.option(k, v);
                        });

        if (sourceTable.primaryKeys().isEmpty()) {
            // for append table with bucket
            if (sourceTable.options().containsKey(BUCKET_KEY.key())) {
                builder.option(BUCKET.key(), sourceTable.options().get(BUCKET.key()));
                builder.option(BUCKET_KEY.key(), sourceTable.options().get(BUCKET_KEY.key()));
            }
        } else {
            // for primary key table, only postpone bucket supports clone
            builder.option(BUCKET.key(), "-2");
        }

        if (!StringUtils.isNullOrWhitespaceOnly(preferFileFormat)) {
            builder.option(CoreOptions.FILE_FORMAT.key(), preferFileFormat);
        }

        try {
            targetCatalog.getTable(tuple.f1);
            if (!cloneIfExists) {
                LOG.info(
                        "Target table '{}' already exists and clone_if_exists is false, skipping clone operation.",
                        tuple.f1);
                return;
            }
        } catch (Catalog.TableNotExistException e) {
            // Table does not exist, proceed to create
        }

        targetCatalog.createTable(tuple.f1, builder.build(), true);

        CloneSchemaInfo cloneSchemaInfo = new CloneSchemaInfo(tuple, true);
        collector.collect(cloneSchemaInfo);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (sourceCatalog != null) {
            this.sourceCatalog.close();
        }
        if (targetCatalog != null) {
            this.targetCatalog.close();
        }
    }
}
