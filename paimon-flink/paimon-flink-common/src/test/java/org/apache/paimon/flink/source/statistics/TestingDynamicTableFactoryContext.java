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

package org.apache.paimon.flink.source.statistics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.factories.DynamicTableFactory;

/** Dynamic table factory context implementation for testing. */
public class TestingDynamicTableFactoryContext implements DynamicTableFactory.Context {
    private final TableConfig configuration;

    private TestingDynamicTableFactoryContext(Configuration configuration) {
        this.configuration = TableConfig.getDefault();
        this.configuration.addConfiguration(configuration);
    }

    @Override
    public ObjectIdentifier getObjectIdentifier() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedCatalogTable getCatalogTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReadableConfig getConfiguration() {
        return configuration;
    }

    @Override
    public ClassLoader getClassLoader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTemporary() {
        throw new UnsupportedOperationException();
    }

    public static TestingTableFactoryContextBuilder builder() {
        return new TestingTableFactoryContextBuilder();
    }

    /** Builder for dynamic table factory context. */
    public static class TestingTableFactoryContextBuilder {
        private final Configuration configuration = new Configuration();

        public TestingTableFactoryContextBuilder configuration(Configuration configuration) {
            this.configuration.addAll(configuration);
            return this;
        }

        public TestingDynamicTableFactoryContext build() {
            return new TestingDynamicTableFactoryContext(configuration);
        }
    }
}
