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

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.table.Table;

import javax.annotation.Nullable;

/** A {@link FlinkTableSource} for metadata table. */
public class MetadataTableSource extends FlinkTableSource {

    private final Table table;

    public MetadataTableSource(Table table) {
        super(table);
        this.table = table;
    }

    public MetadataTableSource(
            Table table,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit) {
        super(table, predicate, projectFields, limit);
        this.table = table;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return SourceProvider.of(new MetadataSource(table, projectFields, predicate, limit));
    }

    @Override
    public DynamicTableSource copy() {
        return new MetadataTableSource(table, predicate, projectFields, limit);
    }
}
