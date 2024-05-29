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
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.statistics.FullFieldStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** A special predicate to filter partition only, just like {@link Predicate}. */
public interface PartitionPredicate {

    boolean test(BinaryRow part);

    boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts);

    @Nullable
    static PartitionPredicate fromPredicate(RowType partitionType, Predicate predicate) {
        if (partitionType.getFieldCount() == 0 || predicate == null) {
            return null;
        }

        return new DefaultPartitionPredicate(predicate);
    }

    @Nullable
    static PartitionPredicate fromMultiple(RowType partitionType, List<BinaryRow> partitions) {
        if (partitionType.getFieldCount() == 0 || partitions.isEmpty()) {
            return null;
        }

        return new MultiplePartitionPredicate(
                new RowDataToObjectArrayConverter(partitionType), new HashSet<>(partitions));
    }

    /** A {@link PartitionPredicate} using {@link Predicate}. */
    class DefaultPartitionPredicate implements PartitionPredicate {

        private final Predicate predicate;

        private DefaultPartitionPredicate(Predicate predicate) {
            this.predicate = predicate;
        }

        @Override
        public boolean test(BinaryRow part) {
            return predicate.test(part);
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            return predicate.test(rowCount, minValues, maxValues, nullCounts);
        }
    }

    /**
     * A {@link PartitionPredicate} optimizing for multiple partitions. Its FieldStats filtering
     * effect may not be as good as {@link DefaultPartitionPredicate}.
     */
    class MultiplePartitionPredicate implements PartitionPredicate {

        private final Set<BinaryRow> partitions;
        private final int fieldNum;
        private final Predicate[] min;
        private final Predicate[] max;

        private MultiplePartitionPredicate(
                RowDataToObjectArrayConverter converter, Set<BinaryRow> partitions) {
            this.partitions = partitions;
            RowType partitionType = converter.rowType();
            this.fieldNum = partitionType.getFieldCount();
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
                Object minValue = stats.minValue();
                Object maxValue = stats.maxValue();

                min[i] = minValue == null ? builder.isNull(i) : builder.greaterOrEqual(i, minValue);
                max[i] = maxValue == null ? builder.isNull(i) : builder.lessOrEqual(i, maxValue);
            }
        }

        @Override
        public boolean test(BinaryRow part) {
            return partitions.contains(part);
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            if (fieldNum == 0) {
                return true;
            }

            for (int i = 0; i < fieldNum; i++) {
                if (!min[i].test(rowCount, minValues, maxValues, nullCounts)
                        || !max[i].test(rowCount, minValues, maxValues, nullCounts)) {
                    return false;
                }
            }
            return true;
        }
    }
}
