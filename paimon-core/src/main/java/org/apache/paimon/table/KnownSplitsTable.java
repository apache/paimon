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

package org.apache.paimon.table;

import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;

/**
 * A table to hold some known data splits. For now, it is only used by internal for Spark engine.
 */
public class KnownSplitsTable implements ReadonlyTable {
    private final InnerTable origin;
    private final DataSplit[] splits;

    KnownSplitsTable(InnerTable origin, DataSplit[] splits) {
        this.origin = origin;
        this.splits = splits;
    }

    public static KnownSplitsTable create(InnerTable origin, DataSplit[] splits) {
        return new KnownSplitsTable(origin, splits);
    }

    public DataSplit[] splits() {
        return splits;
    }

    @Override
    public String name() {
        return origin.name();
    }

    @Override
    public RowType rowType() {
        return origin.rowType();
    }

    @Override
    public List<String> primaryKeys() {
        return origin.primaryKeys();
    }

    @Override
    public List<String> partitionKeys() {
        return origin.partitionKeys();
    }

    @Override
    public Map<String, String> options() {
        return origin.options();
    }

    @Override
    public InnerTableRead newRead() {
        return origin.newRead();
    }

    // ===== unused method ===========================================

    @Override
    public InnerTableScan newScan() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        throw new UnsupportedOperationException();
    }
}
