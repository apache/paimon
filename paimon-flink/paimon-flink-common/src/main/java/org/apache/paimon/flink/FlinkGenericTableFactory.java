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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

/** A table factory to wrap paimon and flink factories. */
public class FlinkGenericTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private final Factory paimon;
    private final Factory flink;

    public FlinkGenericTableFactory(Factory paimon, Factory flink) {
        this.paimon = paimon;
        this.flink = flink;
    }

    @Override
    public String factoryIdentifier() {
        throw new UnsupportedOperationException("Generic factory is only work for catalog.");
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        throw new UnsupportedOperationException("Generic factory is only work for catalog.");
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        throw new UnsupportedOperationException("Generic factory is only work for catalog.");
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        boolean isFlink =
                context.getCatalogTable().getOptions().containsKey(FactoryUtil.CONNECTOR.key());
        if (isFlink) {
            return ((DynamicTableSinkFactory) flink).createDynamicTableSink(context);
        } else {
            return ((DynamicTableSinkFactory) paimon).createDynamicTableSink(context);
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        boolean isFlink =
                context.getCatalogTable().getOptions().containsKey(FactoryUtil.CONNECTOR.key());
        if (isFlink) {
            return ((DynamicTableSourceFactory) flink).createDynamicTableSource(context);
        } else {
            return ((DynamicTableSourceFactory) paimon).createDynamicTableSource(context);
        }
    }
}
