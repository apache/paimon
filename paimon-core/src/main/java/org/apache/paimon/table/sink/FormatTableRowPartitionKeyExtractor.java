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

package org.apache.paimon.table.sink;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import java.util.List;

/** A {@link PartitionKeyExtractor} to {@link InternalRow} for format table. */
public class FormatTableRowPartitionKeyExtractor implements PartitionKeyExtractor<InternalRow> {

    private final Projection partitionProjection;

    public FormatTableRowPartitionKeyExtractor(RowType rowType, List<String> partitionKeys) {
        partitionProjection = CodeGenUtils.newProjection(rowType, partitionKeys);
    }

    @Override
    public BinaryRow partition(InternalRow record) {
        return partitionProjection.apply(record);
    }

    @Override
    public BinaryRow trimmedPrimaryKey(InternalRow record) {
        throw new UnsupportedOperationException();
    }
}
