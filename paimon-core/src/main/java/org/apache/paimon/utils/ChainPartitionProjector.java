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

package org.apache.paimon.utils;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowType;

import java.util.stream.IntStream;

/**
 * Utility to project full partition rows into group (non-chain) and chain parts, and to combine
 * them back into full partition rows.
 *
 * <p>Given partition keys [region, date] with chain partition keys = [date]:
 *
 * <ul>
 *   <li>groupPartition = projection of [region]
 *   <li>chainPartition = projection of [date]
 * </ul>
 */
public class ChainPartitionProjector {
    private final Projection groupProjection;
    private final Projection chainProjection;
    private final InternalRowSerializer fullSerializer;
    private final RowType fullPartitionType;
    private final int groupFieldCount;
    private final int chainFieldCount;

    // Cached projected RowTypes to avoid repeated IntStream allocation on each call.
    private final RowType groupType;
    private final RowType chainType;

    // Pre-built FieldGetter arrays for combinePartition
    private final InternalRow.FieldGetter[] groupFieldGetters;
    private final InternalRow.FieldGetter[] chainFieldGetters;

    /**
     * @param fullPartitionType the full partition RowType
     * @param chainFieldCount number of chain partition fields (counted from the end)
     */
    public ChainPartitionProjector(RowType fullPartitionType, int chainFieldCount) {
        this.fullPartitionType = fullPartitionType;
        int totalFields = fullPartitionType.getFieldCount();
        this.groupFieldCount = totalFields - chainFieldCount;
        this.chainFieldCount = chainFieldCount;

        // group: first groupFieldCount fields
        int[] groupIndices = new int[groupFieldCount];
        for (int i = 0; i < groupFieldCount; i++) {
            groupIndices[i] = i;
        }
        this.groupProjection = CodeGenUtils.newProjection(fullPartitionType, groupIndices);

        // chain: last chainFieldCount fields
        int[] chainIndices = new int[chainFieldCount];
        for (int i = 0; i < chainFieldCount; i++) {
            chainIndices[i] = groupFieldCount + i;
        }
        this.chainProjection = CodeGenUtils.newProjection(fullPartitionType, chainIndices);

        this.fullSerializer = new InternalRowSerializer(fullPartitionType);

        // Cache projected RowTypes once; used by groupPartitionType()/chainPartitionType()
        // and the FieldGetter initialization below.
        this.groupType = fullPartitionType.project(IntStream.range(0, groupFieldCount).toArray());
        this.chainType =
                fullPartitionType.project(
                        IntStream.range(groupFieldCount, groupFieldCount + chainFieldCount)
                                .toArray());

        // Pre-build FieldGetters for type-safe field access from group/chain BinaryRow
        this.groupFieldGetters = new InternalRow.FieldGetter[groupFieldCount];
        for (int i = 0; i < groupFieldCount; i++) {
            groupFieldGetters[i] = InternalRow.createFieldGetter(groupType.getTypeAt(i), i);
        }

        this.chainFieldGetters = new InternalRow.FieldGetter[chainFieldCount];
        for (int i = 0; i < chainFieldCount; i++) {
            chainFieldGetters[i] = InternalRow.createFieldGetter(chainType.getTypeAt(i), i);
        }
    }

    /** Extract group partitions from full partition. */
    public BinaryRow extractGroupPartition(BinaryRow fullPartition) {
        return groupProjection.apply(fullPartition).copy();
    }

    /** Extract chain partitions from full partition. */
    public BinaryRow extractChainPartition(BinaryRow fullPartition) {
        return chainProjection.apply(fullPartition).copy();
    }

    /**
     * Combines the group part and chain part back into a full partition row. Uses
     * InternalRow.createFieldGetter for type-safe field value extraction.
     */
    public BinaryRow combinePartition(BinaryRow groupPart, BinaryRow chainPart) {
        GenericRow combined = new GenericRow(fullPartitionType.getFieldCount());
        for (int i = 0; i < groupFieldCount; i++) {
            combined.setField(i, groupFieldGetters[i].getFieldOrNull(groupPart));
        }
        for (int i = 0; i < chainFieldCount; i++) {
            combined.setField(groupFieldCount + i, chainFieldGetters[i].getFieldOrNull(chainPart));
        }
        return fullSerializer.toBinaryRow(combined).copy();
    }

    public boolean hasGroupPartition() {
        return groupFieldCount > 0;
    }

    public int groupFieldCount() {
        return groupFieldCount;
    }

    public int chainFieldCount() {
        return chainFieldCount;
    }

    public RowType groupPartitionType() {
        return groupType;
    }

    public RowType chainPartitionType() {
        return chainType;
    }

    /**
     * Returns a projection of the chain fields from the given full-partition row <em>without
     * copying</em> the underlying storage.
     *
     * <p>The returned {@link InternalRow} shares the backing bytes of {@code fullPartition} and
     * must not be retained across mutations of that row. Use this method only for transient,
     * read-only comparisons to avoid unnecessary {@link BinaryRow} allocations.
     */
    public InternalRow chainPartitionForCompare(BinaryRow fullPartition) {
        return chainProjection.apply(fullPartition);
    }
}
