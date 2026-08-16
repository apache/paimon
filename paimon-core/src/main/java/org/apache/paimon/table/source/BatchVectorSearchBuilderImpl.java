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
import org.apache.paimon.utils.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link BatchVectorSearchBuilder}. */
public class BatchVectorSearchBuilderImpl implements BatchVectorSearchBuilder {

    private static final long serialVersionUID = 1L;

    protected final FileStoreTable table;

    protected PartitionPredicate partitionFilter;
    protected Predicate filter;
    protected int limit;
    protected DataField vectorColumn;
    protected float[][] vectors;
    protected Map<String, String> options = new HashMap<>();

    public BatchVectorSearchBuilderImpl(InnerTable table) {
        this.table = (FileStoreTable) table;
    }

    @Override
    public BatchVectorSearchBuilder withPartitionFilter(PartitionPredicate partitionFilter) {
        addPartitionFilter(partitionFilter);
        return this;
    }

    @Override
    public BatchVectorSearchBuilder withFilter(Predicate predicate) {
        Pair<Optional<PartitionPredicate>, List<Predicate>> pair =
                splitPartitionPredicatesAndDataPredicates(
                        predicate, table.rowType(), table.partitionKeys());
        if (pair.getLeft().isPresent()) {
            addPartitionFilter(pair.getLeft().get());
        }
        if (!pair.getRight().isEmpty()) {
            Predicate dataFilter = PredicateBuilder.and(pair.getRight());
            if (this.filter == null) {
                this.filter = dataFilter;
            } else {
                this.filter = PredicateBuilder.and(this.filter, dataFilter);
            }
        }
        return this;
    }

    private void addPartitionFilter(PartitionPredicate partitionFilter) {
        if (partitionFilter == null) {
            return;
        }
        if (this.partitionFilter == null) {
            this.partitionFilter = partitionFilter;
        } else {
            this.partitionFilter =
                    PartitionPredicate.and(Arrays.asList(this.partitionFilter, partitionFilter));
        }
    }

    @Override
    public BatchVectorSearchBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public BatchVectorSearchBuilder withVectorColumn(String name) {
        this.vectorColumn = table.rowType().getField(name);
        return this;
    }

    @Override
    public BatchVectorSearchBuilder withVectors(float[][] vectors) {
        this.vectors = vectors;
        return this;
    }

    @Override
    public BatchVectorSearchBuilder withOptions(Map<String, String> options) {
        if (options != null) {
            this.options.putAll(options);
        }
        return this;
    }

    @Override
    public BatchVectorSearchBuilder withOption(String key, String value) {
        this.options.put(key, value);
        return this;
    }

    @Override
    public VectorScan newVectorScan() {
        if (isPrimaryKeyVectorSearch()) {
            return new PrimaryKeyVectorScan(
                    table,
                    vectorColumn.id(),
                    table.coreOptions().primaryKeyVectorIndexType(vectorColumn.name()),
                    partitionFilter,
                    filter);
        }
        return new DataEvolutionVectorScan(table, partitionFilter, filter, vectorColumn, options);
    }

    @Override
    public BatchVectorRead newBatchVectorRead() {
        checkArgument(limit > 0, "Limit must be positive, set via withLimit()");
        checkNotNull(vectorColumn, "Vector column must be set via withVectorColumn()");
        checkArgument(
                vectors != null && vectors.length > 0, "vectors must be set via withVectors()");
        for (float[] vector : vectors) {
            checkNotNull(vector, "Search vector element cannot be null");
        }
        if (isPrimaryKeyVectorSearch()) {
            return new PrimaryKeyBatchVectorRead(
                    table, vectorColumn, vectors, limit, options, filter);
        }
        return new DataEvolutionBatchVectorRead(
                table, partitionFilter, filter, limit, vectorColumn, vectors, options);
    }

    protected boolean isPrimaryKeyVectorSearch() {
        return vectorColumn != null
                && table.coreOptions().primaryKeyVectorIndexColumns().contains(vectorColumn.name());
    }
}
