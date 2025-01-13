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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;
import static org.apache.paimon.predicate.PredicateBuilder.splitOr;

/** Bucket filter push down in scan to skip files. */
public interface BucketSelectConverter {

    int MAX_VALUES = 1000;

    Optional<BiFilter<Integer, Integer>> convert(Predicate predicate);

    static Optional<BiFilter<Integer, Integer>> create(
            Predicate bucketPredicate, RowType bucketKeyType) {
        @SuppressWarnings("unchecked")
        List<Object>[] bucketValues = new List[bucketKeyType.getFieldCount()];

        nextAnd:
        for (Predicate andPredicate : splitAnd(bucketPredicate)) {
            Integer reference = null;
            List<Object> values = new ArrayList<>();
            for (Predicate orPredicate : splitOr(andPredicate)) {
                if (orPredicate instanceof LeafPredicate) {
                    LeafPredicate leaf = (LeafPredicate) orPredicate;
                    if (reference == null || reference == leaf.index()) {
                        reference = leaf.index();
                        if (leaf.function().equals(Equal.INSTANCE)
                                || leaf.function().equals(In.INSTANCE)) {
                            values.addAll(
                                    leaf.literals().stream()
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toList()));
                            continue;
                        }
                    }
                }

                // failed, go to next predicate
                continue nextAnd;
            }
            if (reference != null) {
                if (bucketValues[reference] != null) {
                    // Repeated equals in And?
                    return Optional.empty();
                }

                bucketValues[reference] = values;
            }
        }

        int rowCount = 1;
        for (List<Object> values : bucketValues) {
            if (values == null) {
                return Optional.empty();
            }

            rowCount *= values.size();
            if (rowCount > MAX_VALUES) {
                return Optional.empty();
            }
        }

        InternalRowSerializer serializer = new InternalRowSerializer(bucketKeyType);
        List<Integer> hashCodes = new ArrayList<>();
        assembleRows(
                bucketValues,
                columns -> hashCodes.add(hash(columns, serializer)),
                new ArrayList<>(),
                0);

        return Optional.of(new Selector(hashCodes.stream().mapToInt(i -> i).toArray()));
    }

    static int hash(List<Object> columns, InternalRowSerializer serializer) {
        BinaryRow binaryRow = serializer.toBinaryRow(GenericRow.of(columns.toArray()));
        return KeyAndBucketExtractor.bucketKeyHashCode(binaryRow);
    }

    static void assembleRows(
            List<Object>[] rowValues,
            Consumer<List<Object>> consumer,
            List<Object> stack,
            int columnIndex) {
        List<Object> columnValues = rowValues[columnIndex];
        for (Object value : columnValues) {
            stack.add(value);
            if (columnIndex == rowValues.length - 1) {
                // last column, consume row
                consumer.accept(stack);
            } else {
                assembleRows(rowValues, consumer, stack, columnIndex + 1);
            }
            stack.remove(stack.size() - 1);
        }
    }

    /** Selector to select bucket from {@link Predicate}. */
    @ThreadSafe
    class Selector implements BiFilter<Integer, Integer> {

        private final int[] hashCodes;

        private final Map<Integer, Set<Integer>> buckets = new ConcurrentHashMap<>();

        public Selector(int[] hashCodes) {
            this.hashCodes = hashCodes;
        }

        @Override
        public boolean test(Integer bucket, Integer numBucket) {
            return buckets.computeIfAbsent(numBucket, k -> createBucketSet(numBucket))
                    .contains(bucket);
        }

        @VisibleForTesting
        int[] hashCodes() {
            return hashCodes;
        }

        @VisibleForTesting
        Set<Integer> createBucketSet(int numBucket) {
            ImmutableSet.Builder<Integer> builder = new ImmutableSet.Builder<>();
            for (int hash : hashCodes) {
                builder.add(KeyAndBucketExtractor.bucket(hash, numBucket));
            }
            return builder.build();
        }
    }
}
