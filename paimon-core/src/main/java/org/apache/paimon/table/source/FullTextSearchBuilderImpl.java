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

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.types.DataField;

/** Implementation for {@link FullTextSearchBuilder}. */
public class FullTextSearchBuilderImpl implements FullTextSearchBuilder {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;

    private int limit;
    private DataField textColumn;
    private String queryText;

    public FullTextSearchBuilderImpl(InnerTable table) {
        this.table = (FileStoreTable) table;
    }

    @Override
    public FullTextSearchBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public FullTextSearchBuilder withTextColumn(String name) {
        this.textColumn = table.rowType().getField(name);
        return this;
    }

    @Override
    public FullTextSearchBuilder withQueryText(String queryText) {
        this.queryText = queryText;
        return this;
    }

    @Override
    public FullTextScan newFullTextScan() {
        return new FullTextScanImpl(table, textColumn);
    }

    @Override
    public FullTextRead newFullTextRead() {
        return new FullTextReadImpl(table, limit, textColumn, queryText);
    }
}
