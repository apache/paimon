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

package org.apache.paimon.partition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.statistics.FullFieldStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** A special predicate to filter partition only, just like {@link Predicate}. */
public interface PartitionPredicate {

    boolean test(BinaryRow part);

    boolean test(long rowCount, FieldStats[] fieldStats);

    static PartitionPredicate fromPredicate(RowType partitionType, Predicate predicate) {
        return new DefaultPartitionPredicate(
                new RowDataToObjectArrayConverter(partitionType), predicate);
    }

    static PartitionPredicate fromPartitions(RowType partitionType, List<BinaryRow> partitions) {
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("Partitions can not be empty.");
        }

        if (partitions.size() > 10) {
            return fromMultiple(partitionType, partitions);
        }

        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(partitionType);
        List<Predicate> predicates =
                partitions.stream()
                        .filter(p -> p.getFieldCount() > 0)
                        .map(converter::createEqualPredicate)
                        .collect(Collectors.toList());
        return fromPredicate(partitionType, PredicateBuilder.or(predicates));
    }

    static PartitionPredicate fromMultiple(RowType partitionType, List<BinaryRow> partitions) {
        return new MultiplePartitionPredicate(
                new RowDataToObjectArrayConverter(partitionType), new HashSet<>(partitions));
    }

    class DefaultPartitionPredicate implements PartitionPredicate {

        private final RowDataToObjectArrayConverter converter;
        private final Predicate predicate;

        private DefaultPartitionPredicate(
                RowDataToObjectArrayConverter converter, Predicate predicate) {
            this.converter = converter;
            this.predicate = predicate;
        }

        @Override
        public boolean test(BinaryRow part) {
            return predicate.test(converter.convert(part));
        }

        @Override
        public boolean test(long rowCount, FieldStats[] fieldStats) {
            return predicate.test(rowCount, fieldStats);
        }
    }

    class MultiplePartitionPredicate implements PartitionPredicate {

        private final Set<BinaryRow> partitions;

        private final Predicate[] min;
        private final Predicate[] max;

        private MultiplePartitionPredicate(
                RowDataToObjectArrayConverter converter, Set<BinaryRow> partitions) {
            this.partitions = partitions;
            RowType partitionType = converter.rowType();
            int fieldNum = partitionType.getFieldCount();
            @SuppressWarnings("unchecked")
            Serializer<Object>[] serializers = new Serializer[fieldNum];
            FullFieldStatsCollector[] collectors = new FullFieldStatsCollector[fieldNum];
            min = new Predicate[fieldNum];
            max = new Predicate[fieldNum];
            for (int i = 0; i < fieldNum; i++) {
                serializers[i] = InternalSerializers.create(partitionType.getTypeAt(i));
                collectors[i] = new FullFieldStatsCollector();
            }
            for (BinaryRow part : partitions) {
                Object[] fields = converter.convert(part);
                for (int i = 0; i < fields.length; i++) {
                    collectors[i].collect(fields[i], serializers[i]);
                }
            }
            PredicateBuilder builder = new PredicateBuilder(partitionType);
            for (int i = 0; i < collectors.length; i++) {
                FieldStats stats = collectors[i].result();
                min[i] = builder.greaterOrEqual(i, stats.minValue());
                max[i] = builder.lessOrEqual(i, stats.maxValue());
            }
        }

        @Override
        public boolean test(BinaryRow part) {
            return partitions.contains(part);
        }

        @Override
        public boolean test(long rowCount, FieldStats[] fieldStats) {
            for (int i = 0; i < fieldStats.length; i++) {
                if (min[i].test(rowCount, fieldStats) && max[i].test(rowCount, fieldStats)) {
                    return true;
                }
            }
            return false;
        }
    }
}
