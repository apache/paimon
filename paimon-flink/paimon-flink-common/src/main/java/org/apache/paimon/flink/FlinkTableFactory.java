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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;

import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;

import static org.apache.paimon.CoreOptions.AUTO_CREATE;
import static org.apache.paimon.flink.FlinkCatalogFactory.IDENTIFIER;

/** A paimon {@link DynamicTableFactory} to create source and sink. */
public class FlinkTableFactory extends AbstractFlinkTableFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        createTableIfNeeded(context);
        return super.createDynamicTableSource(context);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        createTableIfNeeded(context);
        return super.createDynamicTableSink(context);
    }

    private void createTableIfNeeded(Context context) {
        ResolvedCatalogTable table = context.getCatalogTable();
        Options options = Options.fromMap(table.getOptions());
        if (options.get(AUTO_CREATE)) {
            try {
                Path tablePath = CoreOptions.path(table.getOptions());
                SchemaManager schemaManager =
                        new SchemaManager(
                                FileIO.get(tablePath, createCatalogContext(context)), tablePath);
                if (!schemaManager.latest().isPresent()) {
                    schemaManager.createTable(FlinkCatalog.fromCatalogTable(table));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
