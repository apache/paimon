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

package org.apache.paimon.table.source;

import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.FullTextQuery;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link FullTextSearchBuilder}. */
public class FullTextSearchBuilderImpl implements FullTextSearchBuilder {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;

    private int limit;
    private FullTextQuery query;
    private PartitionPredicate partitionFilter;

    public FullTextSearchBuilderImpl(InnerTable table) {
        this.table = (FileStoreTable) table;
    }

    @Override
    public FullTextSearchBuilder withPartitionFilter(PartitionPredicate partitionFilter) {
        this.partitionFilter = partitionFilter;
        return this;
    }

    @Override
    public FullTextSearchBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public FullTextSearchBuilder withQuery(FullTextQuery query) {
        this.query = query;
        return this;
    }

    @Override
    public FullTextScan newFullTextScan() {
        return new FullTextScanImpl(table, partitionFilter, textColumns());
    }

    @Override
    public FullTextRead newFullTextRead() {
        checkArgument(limit > 0, "Limit must be positive, set via withLimit()");
        return new FullTextReadImpl(table, partitionFilter, limit, textColumns(), query);
    }

    private List<DataField> textColumns() {
        checkNotNull(query, "Query must be set via withQuery()");
        List<DataField> textColumns = new ArrayList<>();
        for (String columnName : query.columns()) {
            DataField textColumn = table.rowType().getField(columnName);
            checkNotNull(textColumn, "Text column '%s' does not exist.", columnName);
            textColumns.add(textColumn);
        }
        return textColumns;
    }
}
