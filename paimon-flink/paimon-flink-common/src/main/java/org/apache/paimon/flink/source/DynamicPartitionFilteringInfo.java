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

package org.apache.paimon.flink.source;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.Splits;
import org.apache.paimon.types.RowType;

import org.apache.flink.table.connector.source.DynamicFilteringData;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/** Manage dynamic partition filtering fields and table partition row. */
public class DynamicPartitionFilteringInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType partitionRowType;
    private final List<String> dynamicPartitionFilteringFields;

    private transient Projection partitionRowProjection;

    public DynamicPartitionFilteringInfo(
            RowType partitionRowType, List<String> dynamicPartitionFilteringFields) {
        this.partitionRowType = partitionRowType;
        this.dynamicPartitionFilteringFields = dynamicPartitionFilteringFields;
    }

    public Projection getPartitionRowProjection() {
        if (partitionRowProjection == null) {
            partitionRowProjection =
                    CodeGenUtils.newProjection(partitionRowType, dynamicPartitionFilteringFields);
        }

        return partitionRowProjection;
    }

    /**
     * Whether {@code split} may still hold a row {@code dynamicFilteringData} asks for, answered
     * from the partition the plan recorded rather than by opening the split.
     *
     * <p>A mask on a filtering field breaks that shortcut: the reader hands out a value other than
     * the recorded one, so such a split is kept for the join to filter on the masked value.
     */
    public boolean mayMatch(DynamicFilteringData dynamicFilteringData, Split split) {
        TableQueryAuthResult authResult = Splits.authResult(split);
        if (authResult != null
                && !Collections.disjoint(
                        authResult.extractColumnMasking().keySet(),
                        dynamicPartitionFilteringFields)) {
            return true;
        }
        BinaryRow partition = ((DataSplit) Splits.underlying(split)).partition();
        return dynamicFilteringData.contains(
                new FlinkRowData(getPartitionRowProjection().apply(partition)));
    }
}
