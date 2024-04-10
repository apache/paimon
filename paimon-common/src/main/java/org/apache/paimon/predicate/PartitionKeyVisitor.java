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

package org.apache.paimon.predicate;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.InternalRowUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** PartitionKeyVisitor. */
public class PartitionKeyVisitor implements FunctionVisitor<Set<BinaryRow>> {

    private final Set<BinaryRow> partitions;

    private final Map<String, Integer> partitionKeys = new HashMap<>();

    private boolean accurate = true;

    public PartitionKeyVisitor(List<String> partitionKeys, List<BinaryRow> partitions) {
        for (int i = 0; i < partitionKeys.size(); i++) {
            this.partitionKeys.put(partitionKeys.get(i), i);
        }
        this.partitions = new HashSet<>(partitions);
    }

    public boolean isAccurate() {
        return accurate;
    }

    @VisibleForTesting
    public void setAccurate(boolean accurate) {
        this.accurate = accurate;
    }

    private Set<BinaryRow> all() {
        accurate = false;
        return partitions;
    }

    private Set<BinaryRow> filterOrAll(int index, java.util.function.Predicate<BinaryRow> func) {
        if (index == -1) {
            // This isn't a partition column.
            return all();
        } else {
            try {
                return partitions.stream().filter(func).collect(Collectors.toSet());
            } catch (Exception e) {
                return all();
            }
        }
    }

    @Override
    public Set<BinaryRow> visitIsNotNull(FieldRef fieldRef) {
        int index = partitionKeys.getOrDefault(fieldRef.name(), -1);
        return filterOrAll(index, partition -> !partition.isNullAt(index));
    }

    @Override
    public Set<BinaryRow> visitIsNull(FieldRef fieldRef) {
        int index = partitionKeys.getOrDefault(fieldRef.name(), -1);
        return filterOrAll(index, partition -> partition.isNullAt(index));
    }

    @Override
    public Set<BinaryRow> visitStartsWith(FieldRef fieldRef, Object literal) {
        return all();
    }

    @Override
    public Set<BinaryRow> visitLessThan(FieldRef fieldRef, Object literal) {
        int index = partitionKeys.getOrDefault(fieldRef.name(), -1);
        DataType dataType = fieldRef.type();
        return filterOrAll(
                index,
                partition -> {
                    Object value = InternalRowUtils.get(partition, index, dataType);
                    return InternalRowUtils.compare(value, literal, dataType.getTypeRoot()) < 0;
                });
    }

    @Override
    public Set<BinaryRow> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        int index = partitionKeys.getOrDefault(fieldRef.name(), -1);
        DataType dataType = fieldRef.type();
        return filterOrAll(
                index,
                partition -> {
                    Object value = InternalRowUtils.get(partition, index, dataType);
                    return InternalRowUtils.compare(value, literal, dataType.getTypeRoot()) >= 0;
                });
    }

    @Override
    public Set<BinaryRow> visitNotEqual(FieldRef fieldRef, Object literal) {
        int index = partitionKeys.getOrDefault(fieldRef.name(), -1);
        DataType dataType = fieldRef.type();
        return filterOrAll(
                index,
                partition -> {
                    Object value = InternalRowUtils.get(partition, index, dataType);
                    return InternalRowUtils.compare(value, literal, dataType.getTypeRoot()) != 0;
                });
    }

    @Override
    public Set<BinaryRow> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        int index = partitionKeys.getOrDefault(fieldRef.name(), -1);
        DataType dataType = fieldRef.type();
        return filterOrAll(
                index,
                partition -> {
                    Object value = InternalRowUtils.get(partition, index, dataType);
                    return InternalRowUtils.compare(value, literal, dataType.getTypeRoot()) <= 0;
                });
    }

    @Override
    public Set<BinaryRow> visitEqual(FieldRef fieldRef, Object literal) {
        int index = partitionKeys.getOrDefault(fieldRef.name(), -1);
        DataType dataType = fieldRef.type();
        return filterOrAll(
                index,
                partition -> {
                    Object value = InternalRowUtils.get(partition, index, dataType);
                    return InternalRowUtils.compare(value, literal, dataType.getTypeRoot()) == 0;
                });
    }

    @Override
    public Set<BinaryRow> visitGreaterThan(FieldRef fieldRef, Object literal) {
        int index = partitionKeys.getOrDefault(fieldRef.name(), -1);
        DataType dataType = fieldRef.type();
        return filterOrAll(
                index,
                partition -> {
                    Object value = InternalRowUtils.get(partition, index, dataType);
                    return InternalRowUtils.compare(value, literal, dataType.getTypeRoot()) > 0;
                });
    }

    @Override
    public Set<BinaryRow> visitIn(FieldRef fieldRef, List<Object> literals) {
        int index = partitionKeys.getOrDefault(fieldRef.name(), -1);
        DataType dataType = fieldRef.type();
        return filterOrAll(
                index,
                partition -> {
                    Object value = InternalRowUtils.get(partition, index, dataType);
                    return literals.contains(value);
                });
    }

    @Override
    public Set<BinaryRow> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        int index = partitionKeys.getOrDefault(fieldRef.name(), -1);
        DataType dataType = fieldRef.type();
        return filterOrAll(
                index,
                partition -> {
                    Object value = InternalRowUtils.get(partition, index, dataType);
                    return !literals.contains(value);
                });
    }

    @Override
    public Set<BinaryRow> visitAnd(List<Set<BinaryRow>> children) {
        return children.stream().reduce(Sets::intersection).get();
    }

    @Override
    public Set<BinaryRow> visitOr(List<Set<BinaryRow>> children) {
        return children.stream().reduce(Sets::union).get();
    }
}
