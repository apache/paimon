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

package org.apache.flink.table.store.connector;

import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.file.catalog.CatalogLock;

import javax.annotation.Nullable;

import static org.apache.flink.table.store.connector.FlinkCatalogFactory.IDENTIFIER;

/** A table store {@link DynamicTableFactory} to create source and sink. */
public class TableStoreConnectorFactory extends AbstractTableStoreFactory {

    @Nullable private final CatalogLock.Factory lockFactory;

    public TableStoreConnectorFactory() {
        this(null);
    }

    public TableStoreConnectorFactory(@Nullable CatalogLock.Factory lockFactory) {
        this.lockFactory = lockFactory;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public TableStoreSink createDynamicTableSink(Context context) {
        TableStoreSink sink = super.createDynamicTableSink(context);
        sink.setLockFactory(lockFactory);
        return sink;
    }
}
