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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.PartitionKeyExtractor;

/** A {@link PartitionKeyExtractor} to {@link InternalRow}. */
public class CdcRecordPartitionKeyExtractor implements PartitionKeyExtractor<CdcRecord> {

    private final CdcRecordKeyAndBucketExtractor extractor;

    public CdcRecordPartitionKeyExtractor(TableSchema schema) {
        this.extractor = new CdcRecordKeyAndBucketExtractor(schema);
    }

    @Override
    public BinaryRow partition(CdcRecord record) {
        extractor.setRecord(record);
        return extractor.partition();
    }

    @Override
    public BinaryRow trimmedPrimaryKey(CdcRecord record) {
        extractor.setRecord(record);
        return extractor.trimmedPrimaryKey();
    }
}
