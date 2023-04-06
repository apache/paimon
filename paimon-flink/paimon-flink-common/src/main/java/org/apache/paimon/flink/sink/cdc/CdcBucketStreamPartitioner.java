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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BucketComputer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.TypeUtils;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * A {@link StreamPartitioner} which partitions {@link CdcRecord}s according to the hash value of
 * bucket keys (or primary keys if bucket keys are not specified).
 *
 * <p>TODO: merge this class with {@link org.apache.paimon.flink.sink.BucketStreamPartitioner} and
 * refactor {@link BucketComputer} if possible.
 */
public class CdcBucketStreamPartitioner extends StreamPartitioner<CdcRecord> {

    private final int numBuckets;
    private final List<String> bucketKeys;
    private final DataType[] bucketTypes;
    private final List<String> partitionKeys;
    private final DataType[] partitionTypes;
    private final boolean shuffleByPartitionEnable;

    private transient int numberOfChannels;
    private transient Projection bucketProjection;
    private transient Projection partitionProjection;

    public CdcBucketStreamPartitioner(TableSchema schema, boolean shuffleByPartitionEnable) {
        List<String> bucketKeys = schema.originalBucketKeys();
        if (bucketKeys.isEmpty()) {
            bucketKeys = schema.trimmedPrimaryKeys();
        }
        Preconditions.checkArgument(
                bucketKeys.size() > 0, "Either bucket keys or primary keys must be defined");

        this.numBuckets = new CoreOptions(schema.options()).bucket();
        this.bucketKeys = bucketKeys;
        this.bucketTypes = getTypes(this.bucketKeys, schema);
        this.partitionKeys = schema.partitionKeys();
        this.partitionTypes = getTypes(this.partitionKeys, schema);
        this.shuffleByPartitionEnable = shuffleByPartitionEnable;
    }

    private DataType[] getTypes(List<String> keys, TableSchema schema) {
        List<DataType> types = new ArrayList<>();
        for (String key : keys) {
            int idx = schema.fieldNames().indexOf(key);
            types.add(schema.fields().get(idx).type());
        }
        return types.toArray(new DataType[0]);
    }

    @Override
    public void setup(int numberOfChannels) {
        super.setup(numberOfChannels);
        this.numberOfChannels = numberOfChannels;
        this.bucketProjection =
                CodeGenUtils.newProjection(
                        RowType.of(bucketTypes), IntStream.range(0, bucketTypes.length).toArray());
        this.partitionProjection =
                CodeGenUtils.newProjection(
                        RowType.of(partitionTypes),
                        IntStream.range(0, partitionTypes.length).toArray());
    }

    @Override
    public int selectChannel(
            SerializationDelegate<StreamRecord<CdcRecord>> streamRecordSerializationDelegate) {
        CdcRecord record = streamRecordSerializationDelegate.getInstance().getValue();
        BinaryRow bucketKeyRow =
                toBinaryRow(record.fields(), bucketKeys, bucketTypes, bucketProjection);
        int bucket = BucketComputer.bucket(bucketKeyRow.hashCode(), numBuckets);
        if (shuffleByPartitionEnable) {
            BinaryRow partitionKeyRow =
                    toBinaryRow(
                            record.fields(), partitionKeys, partitionTypes, partitionProjection);
            return Math.abs(Objects.hash(bucket, partitionKeyRow.hashCode())) % numberOfChannels;
        } else {
            return bucket % numberOfChannels;
        }
    }

    private BinaryRow toBinaryRow(
            Map<String, String> fields,
            List<String> keys,
            DataType[] types,
            Projection projection) {
        GenericRow genericRow = new GenericRow(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            genericRow.setField(i, TypeUtils.castFromString(fields.get(keys.get(i)), types[i]));
        }
        return projection.apply(genericRow);
    }

    @Override
    public StreamPartitioner<CdcRecord> copy() {
        return null;
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.FULL;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }
}
