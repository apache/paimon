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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;

import org.apache.flink.table.data.RowData;

/** {@link KeyAndBucketExtractor} for {@link RowData}. */
public class RowDataKeyAndBucketExtractor implements KeyAndBucketExtractor<RowData> {

    private final FixedBucketRowKeyExtractor wrapped;

    public RowDataKeyAndBucketExtractor(TableSchema schema) {
        wrapped = new FixedBucketRowKeyExtractor(schema);
    }

    @Override
    public void setRecord(RowData record) {
        wrapped.setRecord(new FlinkRowWrapper(record));
    }

    @Override
    public BinaryRow partition() {
        return wrapped.partition();
    }

    @Override
    public int bucket() {
        return wrapped.bucket();
    }

    @Override
    public BinaryRow trimmedPrimaryKey() {
        return wrapped.trimmedPrimaryKey();
    }

    @Override
    public BinaryRow logPrimaryKey() {
        return wrapped.logPrimaryKey();
    }
}
