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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Writer to write {@link InternalRow} to postpone fixed bucket. */
public class PostponeBatchWriteOperator extends StatelessRowDataStoreWriteOperator {

    private static final long serialVersionUID = 1L;

    private final Map<BinaryRow, Integer> knownNumBuckets;
    private final BucketFunction bucketFunction;

    private transient RowPartitionKeyExtractor partitionKeyExtractor;
    private transient int defaultNumBuckets;
    private transient Projection bucketKeyProjection;

    public PostponeBatchWriteOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser,
            Map<BinaryRow, Integer> knownNumBuckets) {
        super(parameters, table, null, storeSinkWriteProvider, initialCommitUser);
        this.knownNumBuckets = new HashMap<>(knownNumBuckets);
        this.bucketFunction =
                BucketFunction.create(
                        new CoreOptions(table.options()), table.schema().logicalBucketKeyType());
    }

    public void open() throws Exception {
        super.open();

        int sinkParallelism = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
        this.defaultNumBuckets = sinkParallelism <= 0 ? 1 : sinkParallelism;

        TableSchema schema = table.schema();
        this.partitionKeyExtractor = new RowPartitionKeyExtractor(schema);
        this.bucketKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.bucketKeys()));
        ((StoreSinkWriteImpl) write).getWrite().getWrite().withIgnoreNumBucketCheck(true);
    }

    @Override
    @Nullable
    protected SinkRecord write(InternalRow row) throws Exception {
        BinaryRow partition = partitionKeyExtractor.partition(row);
        int numBuckets = knownNumBuckets.computeIfAbsent(partition.copy(), p -> defaultNumBuckets);
        int bucket = bucketFunction.bucket(bucketKeyProjection.apply(row), numBuckets);
        return write.write(row, bucket);
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = new ArrayList<>();
        for (Committable committable : super.prepareCommit(waitCompaction, checkpointId)) {
            if (committable.kind() == Committable.Kind.FILE) {
                CommitMessageImpl message = (CommitMessageImpl) committable.wrappedCommittable();
                committables.add(
                        new Committable(
                                committable.checkpointId(),
                                committable.kind(),
                                new CommitMessageImpl(
                                        message.partition(),
                                        message.bucket(),
                                        checkNotNull(knownNumBuckets.get(message.partition())),
                                        message.newFilesIncrement(),
                                        message.compactIncrement())));
            } else {
                committables.add(committable);
            }
        }

        return committables;
    }
}
