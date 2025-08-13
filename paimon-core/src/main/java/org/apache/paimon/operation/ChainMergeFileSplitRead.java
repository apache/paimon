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

package org.apache.paimon.operation;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ChainDataSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;

/** A specific implementation for {@link MergeFileSplitRead} on chain table. */
public class ChainMergeFileSplitRead extends MergeFileSplitRead {

    public ChainMergeFileSplitRead(MergeFileSplitRead mergeFileSplitRead) {
        super(mergeFileSplitRead);
    }

    @Override
    public RecordReader<KeyValue> createReader(DataSplit split) throws IOException {
        Preconditions.checkArgument(
                split instanceof ChainDataSplit, "split must be ChainDataSplit");
        if (!split.beforeFiles().isEmpty()
                || split.isStreaming()
                || split.bucket() == BucketMode.POSTPONE_BUCKET) {
            throw new IllegalArgumentException(
                    String.format(
                            "This read can only enable accept split on bucket"
                                    + " mode and without before files %s, streaming %s, bucket%s  ",
                            split.beforeFiles(), split.isStreaming(), split.bucket()));
        }
        return createMergeReader(
                split.partition(),
                split.bucket(),
                split.dataFiles(),
                split.deletionFiles().orElse(null),
                forceKeepDelete,
                split);
    }

    @Override
    protected KeyValueFileReaderFactory getOverlappedSectionFactory(
            DataSplit split, BinaryRow partition, int bucket, DeletionVector.Factory dvFactory) {
        Preconditions.checkArgument(
                split instanceof ChainDataSplit, "split must be ChainDataSplit");
        KeyValueFileReaderFactory factory =
                super.getOverlappedSectionFactory(split, null, bucket, dvFactory);
        return new ChainKeyValueFileReaderFactory(
                factory, split.readPartition(), split.fileBranchMapping());
    }

    @Override
    public KeyValueFileReaderFactory getNonOverlappedSectionFactory(
            DataSplit split, BinaryRow partition, int bucket, DeletionVector.Factory dvFactory) {
        Preconditions.checkArgument(
                split instanceof ChainDataSplit, "split must be ChainDataSplit");
        KeyValueFileReaderFactory factory =
                super.getNonOverlappedSectionFactory(split, null, bucket, dvFactory);
        return new ChainKeyValueFileReaderFactory(
                factory, split.readPartition(), split.fileBranchMapping());
    }
}
