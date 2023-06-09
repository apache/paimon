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
import org.apache.paimon.schema.TableSchema;

/** A {@link PartitionKeyExtractor} to {@link InternalRow}. */
public class RowPartitionKeyExtractor implements PartitionKeyExtractor<InternalRow> {

    private final Projection partitionProjection;
    private final Projection trimmedPrimaryKeyProjection;

    public RowPartitionKeyExtractor(TableSchema schema) {
        partitionProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.partitionKeys()));
        trimmedPrimaryKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.trimmedPrimaryKeys()));
    }

    @Override
    public BinaryRow partition(InternalRow record) {
        return partitionProjection.apply(record);
    }

    @Override
    public BinaryRow trimmedPrimaryKey(InternalRow record) {
        return trimmedPrimaryKeyProjection.apply(record);
    }
}
