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

package org.apache.flink.table.store.table.source;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.CompoundPredicate;
import org.apache.flink.table.store.file.predicate.LeafPredicate;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** An abstraction layer above {@link FileStoreScan} to provide input split generation. */
public abstract class TableScan {

    private final FileStoreScan scan;
    private final Schema schema;
    private final FileStorePathFactory pathFactory;

    protected TableScan(FileStoreScan scan, Schema schema, FileStorePathFactory pathFactory) {
        this.scan = scan;
        this.schema = schema;
        this.pathFactory = pathFactory;
    }

    public TableScan withSnapshot(long snapshotId) {
        scan.withSnapshot(snapshotId);
        return this;
    }

    public TableScan withFilter(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return this;
        }
        return withFilter(PredicateBuilder.and(predicates));
    }

    public TableScan withFilter(Predicate predicate) {
        List<String> partitionKeys = schema.partitionKeys();
        int[] fieldIdxToPartitionIdx =
                schema.fields().stream().mapToInt(f -> partitionKeys.indexOf(f.name())).toArray();

        List<Predicate> partitionFilters = new ArrayList<>();
        List<Predicate> nonPartitionFilters = new ArrayList<>();
        for (Predicate p : PredicateBuilder.splitAnd(predicate)) {
            Optional<Predicate> mapped = mapFilterFields(p, fieldIdxToPartitionIdx);
            if (mapped.isPresent()) {
                partitionFilters.add(mapped.get());
            } else {
                nonPartitionFilters.add(p);
            }
        }

        if (partitionFilters.size() > 0) {
            scan.withPartitionFilter(PredicateBuilder.and(partitionFilters));
        }
        if (nonPartitionFilters.size() > 0) {
            withNonPartitionFilter(PredicateBuilder.and(nonPartitionFilters));
        }
        return this;
    }

    public TableScan withIncremental(boolean isIncremental) {
        scan.withIncremental(isIncremental);
        return this;
    }

    public Plan plan() {
        FileStoreScan.Plan plan = scan.plan();
        return new Plan(
                plan.snapshotId(),
                new DefaultSplitGenerator(pathFactory).generate(plan.groupByPartFiles()));
    }

    protected abstract void withNonPartitionFilter(Predicate predicate);

    protected Optional<Predicate> mapFilterFields(Predicate predicate, int[] fieldIdxMapping) {
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            List<Predicate> children = new ArrayList<>();
            for (Predicate child : compoundPredicate.children()) {
                Optional<Predicate> mapped = mapFilterFields(child, fieldIdxMapping);
                if (mapped.isPresent()) {
                    children.add(mapped.get());
                } else {
                    return Optional.empty();
                }
            }
            return Optional.of(new CompoundPredicate(compoundPredicate.function(), children));
        } else {
            LeafPredicate leafPredicate = (LeafPredicate) predicate;
            int mapped = fieldIdxMapping[leafPredicate.index()];
            if (mapped >= 0) {
                return Optional.of(
                        new LeafPredicate(
                                leafPredicate.function(), mapped, leafPredicate.literal()));
            } else {
                return Optional.empty();
            }
        }
    }

    /** Scanning plan containing snapshot ID and input splits. */
    public static class Plan {

        @Nullable public final Long snapshotId;
        public final List<Split> splits;

        @VisibleForTesting
        public Plan(@Nullable Long snapshotId, List<Split> splits) {
            this.snapshotId = snapshotId;
            this.splits = splits;
        }
    }
}
