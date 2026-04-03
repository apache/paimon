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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.types.DataField;

import static org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicate;

/** Implementation for {@link VectorSearchBuilder}. */
public class VectorSearchBuilderImpl implements VectorSearchBuilder {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;

    private PartitionPredicate partitionFilter;
    private Predicate filter;
    private int limit;
    private DataField vectorColumn;
    private float[] vector;

    public VectorSearchBuilderImpl(InnerTable table) {
        this.table = (FileStoreTable) table;
    }

    @Override
    public VectorSearchBuilder withPartitionFilter(PartitionPredicate partitionFilter) {
        this.partitionFilter = partitionFilter;
        return this;
    }

    @Override
    public VectorSearchBuilder withFilter(Predicate predicate) {
        if (this.filter == null) {
            this.filter = predicate;
        } else {
            this.filter = PredicateBuilder.and(this.filter, predicate);
        }
        splitPartitionPredicate(predicate, table.rowType(), table.partitionKeys())
                .ifPresent(value -> this.partitionFilter = value);
        return this;
    }

    @Override
    public VectorSearchBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public VectorSearchBuilder withVectorColumn(String name) {
        this.vectorColumn = table.rowType().getField(name);
        return this;
    }

    @Override
    public VectorSearchBuilder withVector(float[] vector) {
        this.vector = vector;
        return this;
    }

    @Override
    public VectorScan newVectorScan() {
        return new VectorScanImpl(table, partitionFilter, filter, vectorColumn);
    }

    @Override
    public VectorRead newVectorRead() {
        return new VectorReadImpl(table, filter, limit, vectorColumn, vector);
    }
}
