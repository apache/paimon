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

package org.apache.paimon.flink.clone.spits;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.FlinkCatalogFactory.createPaimonCatalog;
import static org.apache.paimon.flink.clone.files.ListCloneFilesFunction.getPartitionPredicate;

/** List splits for table. */
public class ListCloneSplitsFunction
        extends ProcessFunction<Tuple2<Identifier, Identifier>, CloneSplitInfo> {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;
    @Nullable private final String whereSql;

    private transient Catalog sourceCatalog;
    private transient Catalog targetCatalog;

    public ListCloneSplitsFunction(
            Map<String, String> sourceCatalogConfig,
            Map<String, String> targetCatalogConfig,
            @Nullable String whereSql) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
        this.whereSql = whereSql;
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
            ProcessFunction<Tuple2<Identifier, Identifier>, CloneSplitInfo>.Context context,
            Collector<CloneSplitInfo> collector)
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

        targetCatalog.createTable(tuple.f1, builder.build(), true);

        PartitionPredicate predicate =
                getPartitionPredicate(
                        whereSql,
                        sourceTable.rowType().project(sourceTable.partitionKeys()),
                        tuple.f0);
        TableScan scan = sourceTable.newReadBuilder().withPartitionFilter(predicate).newScan();
        List<Split> splits = scan.plan().splits();
        for (Split split : splits) {
            CloneSplitInfo splitInfo = new CloneSplitInfo(tuple.f0, tuple.f1, split);
            collector.collect(splitInfo);
        }
    }
}
