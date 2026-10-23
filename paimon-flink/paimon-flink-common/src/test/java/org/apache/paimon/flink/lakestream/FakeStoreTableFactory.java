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

package org.apache.paimon.flink.lakestream;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The fake lake-stream store's table factory, playing the role that a store's catalog-scoped
 * factory (e.g. Fluss's) plays in production. {@link FakeStoreCatalog#getFactory()} returns it, and
 * Paimon's lake-stream routing delegates {@code $rt} source creation and regular sink creation
 * through it.
 *
 * <p>The contexts it is invoked with are recorded statically so tests can assert the adaptation
 * Paimon performs (base identifier without {@code $rt}, store table options, prefix-removed store
 * connection options).
 */
public class FakeStoreTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = FakeStoreCatalogFactory.IDENTIFIER;

    private static final List<DynamicTableFactory.Context> SOURCE_CONTEXTS =
            Collections.synchronizedList(new ArrayList<DynamicTableFactory.Context>());
    private static final List<DynamicTableFactory.Context> SINK_CONTEXTS =
            Collections.synchronizedList(new ArrayList<DynamicTableFactory.Context>());

    private final FakeStoreSourceFactory sourceFactory = new FakeStoreSourceFactory();
    private final FakeStoreSinkFactory sinkFactory = new FakeStoreSinkFactory();

    public static List<DynamicTableFactory.Context> sourceContexts() {
        synchronized (SOURCE_CONTEXTS) {
            return new ArrayList<>(SOURCE_CONTEXTS);
        }
    }

    public static List<DynamicTableFactory.Context> sinkContexts() {
        synchronized (SINK_CONTEXTS) {
            return new ArrayList<>(SINK_CONTEXTS);
        }
    }

    public static void reset() {
        SOURCE_CONTEXTS.clear();
        SINK_CONTEXTS.clear();
        FakeStoreSink.resetAbilityCalls();
        FakeStoreData.reset();
    }

    static void recordSourceContext(DynamicTableFactory.Context context) {
        SOURCE_CONTEXTS.add(context);
    }

    static void recordSinkContext(DynamicTableFactory.Context context) {
        SINK_CONTEXTS.add(context);
    }

    static String tableKeyOf(DynamicTableFactory.Context context) {
        return context.getObjectIdentifier().getDatabaseName()
                + "."
                + context.getObjectIdentifier().getObjectName();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return sourceFactory.createDynamicTableSource(context);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return sinkFactory.createDynamicTableSink(context);
    }

    /** Source-only variant, to test Paimon's factory-type validation. */
    static class FakeStoreSourceFactory implements DynamicTableSourceFactory {

        @Override
        public String factoryIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.emptySet();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return Collections.emptySet();
        }

        @Override
        public DynamicTableSource createDynamicTableSource(Context context) {
            recordSourceContext(context);
            return new FakeStoreSource(
                    tableKeyOf(context),
                    (RowType)
                            context.getCatalogTable()
                                    .getResolvedSchema()
                                    .toPhysicalRowDataType()
                                    .getLogicalType());
        }
    }

    /** Sink-only variant, to test Paimon's factory-type validation. */
    static class FakeStoreSinkFactory implements DynamicTableSinkFactory {

        @Override
        public String factoryIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.emptySet();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return Collections.emptySet();
        }

        @Override
        public DynamicTableSink createDynamicTableSink(Context context) {
            recordSinkContext(context);
            return new FakeStoreSink(tableKeyOf(context));
        }
    }
}
