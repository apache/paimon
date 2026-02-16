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

import java.io.Serializable;

/** {@link KeyAndBucketExtractor} for {@link InternalRow}. */
public abstract class RowKeyExtractor implements KeyAndBucketExtractor<InternalRow>, Serializable {

    private static final long serialVersionUID = 1L;

    private transient Projection logPrimaryKeyProjection;
    private transient RowPartitionKeyExtractor partitionKeyExtractor;

    protected final TableSchema schema;

    protected InternalRow record;

    private BinaryRow partition;
    private BinaryRow trimmedPrimaryKey;
    private BinaryRow logPrimaryKey;

    public RowKeyExtractor(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public void setRecord(InternalRow record) {
        this.record = record;
        this.partition = null;
        this.trimmedPrimaryKey = null;
        this.logPrimaryKey = null;
    }

    @Override
    public BinaryRow partition() {
        if (partition == null) {
            partition = partitionKeyExtractor().partition(record);
        }
        return partition;
    }

    @Override
    public BinaryRow trimmedPrimaryKey() {
        if (trimmedPrimaryKey == null) {
            trimmedPrimaryKey = partitionKeyExtractor().trimmedPrimaryKey(record);
        }
        return trimmedPrimaryKey;
    }

    @Override
    public BinaryRow logPrimaryKey() {
        if (logPrimaryKey == null) {
            logPrimaryKey = logPrimaryKeyProjection().apply(record);
        }
        return logPrimaryKey;
    }

    private Projection logPrimaryKeyProjection() {
        if (logPrimaryKeyProjection == null) {
            logPrimaryKeyProjection =
                    CodeGenUtils.newProjection(
                            schema.logicalRowType(), schema.projection(schema.primaryKeys()));
        }
        return logPrimaryKeyProjection;
    }

    private RowPartitionKeyExtractor partitionKeyExtractor() {
        if (partitionKeyExtractor == null) {
            partitionKeyExtractor = new RowPartitionKeyExtractor(schema);
        }
        return partitionKeyExtractor;
    }
}
