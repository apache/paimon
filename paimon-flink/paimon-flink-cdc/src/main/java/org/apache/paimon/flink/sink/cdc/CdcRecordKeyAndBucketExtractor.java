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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.stream.IntStream;

import static org.apache.paimon.flink.sink.cdc.CdcRecordUtils.projectAsInsert;

/** {@link KeyAndBucketExtractor} for {@link CdcRecord}. */
public class CdcRecordKeyAndBucketExtractor implements KeyAndBucketExtractor<CdcRecord> {

    private final int numBuckets;

    private final List<DataField> partitionFields;
    private final Projection partitionProjection;
    private final List<DataField> bucketKeyFields;
    private final Projection bucketKeyProjection;
    private final List<DataField> trimmedPKFields;
    private final Projection trimmedPKProjection;

    private CdcRecord record;

    private BinaryRow partition;
    private BinaryRow trimmedPK;
    private BinaryRow bucketKey;
    private Integer bucket;

    public CdcRecordKeyAndBucketExtractor(TableSchema schema) {
        numBuckets = new CoreOptions(schema.options()).bucket();

        RowType partitionType = schema.logicalPartitionType();
        this.partitionFields = partitionType.getFields();
        this.partitionProjection =
                CodeGenUtils.newProjection(
                        partitionType, IntStream.range(0, partitionType.getFieldCount()).toArray());

        RowType bucketKeyType = schema.logicalBucketKeyType();
        this.bucketKeyFields = bucketKeyType.getFields();
        this.bucketKeyProjection =
                CodeGenUtils.newProjection(
                        bucketKeyType, IntStream.range(0, bucketKeyType.getFieldCount()).toArray());

        this.trimmedPKFields = schema.trimmedPrimaryKeysFields();
        this.trimmedPKProjection =
                CodeGenUtils.newProjection(
                        new RowType(trimmedPKFields),
                        IntStream.range(0, trimmedPKFields.size()).toArray());
    }

    @Override
    public void setRecord(CdcRecord record) {
        this.record = record;

        this.partition = null;
        this.bucketKey = null;
        this.trimmedPK = null;
        this.bucket = null;
    }

    @Override
    public BinaryRow partition() {
        if (partition == null) {
            partition = partitionProjection.apply(projectAsInsert(record, partitionFields));
        }
        return partition;
    }

    @Override
    public int bucket() {
        if (bucketKey == null) {
            bucketKey = bucketKeyProjection.apply(projectAsInsert(record, bucketKeyFields));
        }
        if (bucket == null) {
            bucket =
                    KeyAndBucketExtractor.bucket(
                            KeyAndBucketExtractor.bucketKeyHashCode(bucketKey), numBuckets);
        }
        return bucket;
    }

    @Override
    public BinaryRow trimmedPrimaryKey() {
        if (trimmedPK == null) {
            trimmedPK = trimmedPKProjection.apply(projectAsInsert(record, trimmedPKFields));
        }
        return trimmedPK;
    }

    @Override
    public BinaryRow logPrimaryKey() {
        throw new UnsupportedOperationException();
    }
}
