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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * A table wrapper to hold vector search information. This is used by Spark engine to pass vector
 * search pushdown information from logical plan optimization to physical plan execution.
 */
public class VectorSearchTable implements ReadonlyTable {

    private final InnerTable origin;
    private final VectorSearch vectorSearch;

    VectorSearchTable(InnerTable origin, VectorSearch vectorSearch) {
        this.origin = origin;
        this.vectorSearch = vectorSearch;
    }

    public static VectorSearchTable create(InnerTable origin, VectorSearch vectorSearch) {
        return new VectorSearchTable(origin, vectorSearch);
    }

    @Nullable
    public VectorSearch vectorSearch() {
        return vectorSearch;
    }

    public InnerTable origin() {
        return origin;
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
    public FileIO fileIO() {
        return origin.fileIO();
    }

    @Override
    public InnerTableRead newRead() {
        return origin.newRead();
    }

    @Override
    public InnerTableScan newScan() {
        return origin.newScan();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new VectorSearchTable((InnerTable) origin.copy(dynamicOptions), vectorSearch);
    }
}
