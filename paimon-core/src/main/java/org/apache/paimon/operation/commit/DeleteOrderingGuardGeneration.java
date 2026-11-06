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

package org.apache.paimon.operation.commit;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeleteOrderingGuardFile;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Persistent, scoped generations for writes that must not cross an ordering-history reset. */
public class DeleteOrderingGuardGeneration {

    public static final String SNAPSHOT_PROPERTY = "delete-ordering-guards.generations";
    private static final String VERSION = "version";
    private static final String ALL_PARTITIONS = "*";
    private static final String INITIAL_GENERATION = "0";

    private DeleteOrderingGuardGeneration() {}

    public static boolean isTracked(@Nullable Snapshot snapshot) {
        return snapshot != null
                && snapshot.properties() != null
                && snapshot.properties().containsKey(SNAPSHOT_PROPERTY);
    }

    @Nullable
    public static String forWriter(
            @Nullable Snapshot restoredSnapshot, BinaryRow partition, boolean captureEnabled) {
        return captureEnabled
                        || isTracked(restoredSnapshot)
                        || DeleteOrderingGuardFile.mayContainGuards(restoredSnapshot)
                ? generation(restoredSnapshot, partition)
                : null;
    }

    /** No old snapshot is needed: the current snapshot retains all effective reset scopes. */
    public static String generation(@Nullable Snapshot snapshot, BinaryRow partition) {
        String result = INITIAL_GENERATION;
        for (Map.Entry<String, String> rule : read(snapshot).entrySet()) {
            if (VERSION.equals(rule.getKey())) {
                continue;
            }
            if (ALL_PARTITIONS.equals(rule.getKey())
                    || JsonSerdeUtil.fromJson(rule.getKey(), Predicate.class).test(partition)) {
                result = rule.getValue();
            }
        }
        return result;
    }

    public static void validate(@Nullable Snapshot latestSnapshot, List<CommitMessage> messages) {
        if (!isTracked(latestSnapshot)) {
            return;
        }
        for (CommitMessage message : messages) {
            if (((CommitMessageImpl) message).isEmpty()) {
                continue;
            }
            String expected = message.deleteOrderingGuardGeneration();
            // Pre-generation messages belong to the initial history only. They must never be
            // silently rebased over a reset, even when they contain only ordinary append files.
            if (expected == null) {
                expected = INITIAL_GENERATION;
            }
            checkState(
                    expected.equals(generation(latestSnapshot, message.partition())),
                    "Delete ordering guard generation changed for partition %s. "
                            + "Reopen the writer after the ordering-history reset.",
                    message.partition());
        }
    }

    /** Check again against each publication attempt, including optimistic commit retries. */
    public static CommitChangesProvider fence(
            CommitChangesProvider delegate, List<CommitMessage> messages) {
        return new CommitChangesProvider() {
            @Override
            public CommitChanges provide(@Nullable Snapshot latestSnapshot) {
                validate(latestSnapshot, messages);
                return delegate.provide(latestSnapshot);
            }

            @Override
            public Set<Long> rebasedReassignments() {
                return delegate.rebasedReassignments();
            }

            @Override
            public boolean resetsDeleteOrderingGuards() {
                return delegate.resetsDeleteOrderingGuards();
            }

            @Nullable
            @Override
            public PartitionPredicate deleteOrderingGuardResetFilter() {
                return delegate.deleteOrderingGuardResetFilter();
            }
        };
    }

    /** Inherit the state on every snapshot; atomically advance it with an explicit scoped reset. */
    public static Map<String, String> merge(
            @Nullable Map<String, String> properties,
            @Nullable Snapshot latestSnapshot,
            boolean captureEnabled,
            boolean reset,
            @Nullable PartitionPredicate resetFilter,
            RowType partitionType) {
        Map<String, String> result =
                properties == null ? new HashMap<>() : new HashMap<>(properties);
        // This is internal commit state, not a property supplied by an individual committable.
        result.remove(SNAPSHOT_PROPERTY);
        if (!captureEnabled
                && !isTracked(latestSnapshot)
                && !DeleteOrderingGuardFile.mayContainGuards(latestSnapshot)) {
            return result;
        }
        LinkedHashMap<String, String> generations = read(latestSnapshot);
        if (reset) {
            Predicate predicate = toPredicate(resetFilter, partitionType);
            String scope = predicate == null ? ALL_PARTITIONS : JsonSerdeUtil.toFlatJson(predicate);
            if (predicate == null) {
                generations.clear();
                generations.put(VERSION, "1");
            }
            // The last matching rule wins, including overlapping partial partition specs.
            generations.remove(scope);
            generations.put(scope, UUID.randomUUID().toString());
        }
        result.put(SNAPSHOT_PROPERTY, JsonSerdeUtil.toFlatJson(generations));
        return result;
    }

    private static LinkedHashMap<String, String> read(@Nullable Snapshot snapshot) {
        LinkedHashMap<String, String> result;
        if (!isTracked(snapshot)) {
            result = new LinkedHashMap<>();
            result.put(VERSION, "1");
            return result;
        }
        result =
                JsonSerdeUtil.fromJson(
                        snapshot.properties().get(SNAPSHOT_PROPERTY),
                        new TypeReference<LinkedHashMap<String, String>>() {});
        checkArgument(
                result != null && "1".equals(result.get(VERSION)),
                "Unsupported delete ordering guard generation state.");
        for (String token : result.values()) {
            checkArgument(token != null && !token.isEmpty(), "Invalid guard generation token.");
        }
        return result;
    }

    @Nullable
    private static Predicate toPredicate(
            @Nullable PartitionPredicate filter, RowType partitionType) {
        if (filter == null || filter == PartitionPredicate.ALWAYS_TRUE) {
            return null;
        }
        if (filter instanceof PartitionPredicate.DefaultPartitionPredicate) {
            return ((PartitionPredicate.DefaultPartitionPredicate) filter).predicate();
        }
        if (filter instanceof PartitionPredicate.MultiplePartitionPredicate) {
            return PartitionPredicate.createPartitionPredicate(
                    partitionType,
                    new RowDataToObjectArrayConverter(partitionType),
                    new ArrayList<>(
                            ((PartitionPredicate.MultiplePartitionPredicate) filter).partitions()));
        }
        if (filter instanceof PartitionPredicate.AndPartitionPredicate) {
            List<Predicate> predicates = new ArrayList<>();
            for (PartitionPredicate child :
                    ((PartitionPredicate.AndPartitionPredicate) filter).predicates()) {
                Predicate predicate = toPredicate(child, partitionType);
                if (predicate != null) {
                    predicates.add(predicate);
                }
            }
            return predicates.isEmpty() ? null : PredicateBuilder.and(predicates);
        }
        throw new IllegalArgumentException(
                "Unsupported guard generation reset predicate: " + filter);
    }
}
