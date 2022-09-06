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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.store.CoreOptions.ChangelogProducer;
import org.apache.flink.table.store.CoreOptions.LogChangelogMode;
import org.apache.flink.table.store.CoreOptions.LogConsistency;
import org.apache.flink.table.store.connector.FlinkConnectorOptions;
import org.apache.flink.table.store.connector.TableStoreDataStreamScanProvider;
import org.apache.flink.table.store.connector.lookup.FileStoreLookupFunction;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.log.LogSourceProvider;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.store.table.AppendOnlyFileStoreTable;
import org.apache.flink.table.store.table.ChangelogValueCountFileStoreTable;
import org.apache.flink.table.store.table.ChangelogWithKeyFileStoreTable;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.utils.Projection;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.table.store.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.flink.table.store.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.flink.table.store.CoreOptions.LOG_CONSISTENCY;
import static org.apache.flink.table.store.CoreOptions.LOG_SCAN_REMOVE_NORMALIZE;

/**
 * Table source to create {@link FileStoreSource} under batch mode or change-tracking is disabled.
 * For streaming mode with change-tracking enabled and FULL scan mode, it will create a {@link
 * org.apache.flink.connector.base.source.hybrid.HybridSource} of {@link FileStoreSource} and kafka
 * log source created by {@link LogSourceProvider}.
 */
public class TableStoreSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsFilterPushDown,
                SupportsProjectionPushDown,
                SupportsWatermarkPushDown {

    private final ObjectIdentifier tableIdentifier;
    private final FileStoreTable table;
    private final boolean streaming;
    private final DynamicTableFactory.Context context;
    @Nullable private final LogStoreTableFactory logStoreTableFactory;

    @Nullable private Predicate predicate;
    @Nullable private int[][] projectFields;

    @Nullable private WatermarkStrategy<RowData> watermarkStrategy;

    public TableStoreSource(
            ObjectIdentifier tableIdentifier,
            FileStoreTable table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        this(tableIdentifier, table, streaming, context, logStoreTableFactory, null, null, null);
    }

    private TableStoreSource(
            ObjectIdentifier tableIdentifier,
            FileStoreTable table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.streaming = streaming;
        this.context = context;
        this.logStoreTableFactory = logStoreTableFactory;
        this.predicate = predicate;
        this.projectFields = projectFields;
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (!streaming) {
            // batch merge all, return insert only
            return ChangelogMode.insertOnly();
        }

        if (table instanceof AppendOnlyFileStoreTable) {
            return ChangelogMode.insertOnly();
        } else if (table instanceof ChangelogValueCountFileStoreTable) {
            return ChangelogMode.all();
        } else if (table instanceof ChangelogWithKeyFileStoreTable) {
            Configuration options = Configuration.fromMap(table.schema().options());

            if (options.get(LOG_SCAN_REMOVE_NORMALIZE)) {
                return ChangelogMode.all();
            }

            if (logStoreTableFactory == null
                    && options.get(CHANGELOG_PRODUCER) != ChangelogProducer.NONE) {
                return ChangelogMode.all();
            }

            // optimization: transaction consistency and all changelog mode avoid the generation of
            // normalized nodes. See TableStoreSink.getChangelogMode validation.
            return options.get(LOG_CONSISTENCY) == LogConsistency.TRANSACTIONAL
                            && options.get(LOG_CHANGELOG_MODE) == LogChangelogMode.ALL
                    ? ChangelogMode.all()
                    : ChangelogMode.upsert();
        } else {
            throw new UnsupportedOperationException(
                    "Unknown FileStoreTable subclass " + table.getClass().getName());
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        LogSourceProvider logSourceProvider = null;
        if (logStoreTableFactory != null) {
            logSourceProvider =
                    logStoreTableFactory.createSourceProvider(context, scanContext, projectFields);
        }

        FlinkSourceBuilder sourceBuilder =
                new FlinkSourceBuilder(tableIdentifier, table)
                        .withContinuousMode(streaming)
                        .withLogSourceProvider(logSourceProvider)
                        .withProjection(projectFields)
                        .withPredicate(predicate)
                        .withParallelism(
                                Configuration.fromMap(table.schema().options())
                                        .get(FlinkConnectorOptions.SCAN_PARALLELISM))
                        .withWatermarkStrategy(watermarkStrategy);

        return new TableStoreDataStreamScanProvider(
                !streaming, env -> sourceBuilder.withEnv(env).build());
    }

    @Override
    public DynamicTableSource copy() {
        return new TableStoreSource(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                watermarkStrategy);
    }

    @Override
    public String asSummaryString() {
        return "TableStoreSource";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<Predicate> converted = new ArrayList<>();
        RowType rowType = table.schema().logicalRowType();
        for (ResolvedExpression filter : filters) {
            PredicateConverter.convert(rowType, filter).ifPresent(converted::add);
        }
        predicate = converted.isEmpty() ? null : PredicateBuilder.and(converted);
        return Result.of(filters, filters);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.projectFields = projectedFields;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        int[] projection =
                projectFields == null
                        ? IntStream.range(0, table.schema().fields().size()).toArray()
                        : Projection.of(projectFields).toTopLevelIndexes();
        int[] joinKey = Projection.of(context.getKeys()).toTopLevelIndexes();
        return TableFunctionProvider.of(
                new FileStoreLookupFunction(table, projection, joinKey, predicate));
    }
}
