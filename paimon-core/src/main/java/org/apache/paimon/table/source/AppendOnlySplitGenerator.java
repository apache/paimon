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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.BinPacking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.append.AppendOnlyCompactManager.fileComparator;

/** Append only implementation of {@link SplitGenerator}. */
public class AppendOnlySplitGenerator implements SplitGenerator {

    private final long targetSplitSize;
    private final long openFileCost;
    private final BucketMode bucketMode;

    public AppendOnlySplitGenerator(
            long targetSplitSize, long openFileCost, BucketMode bucketMode) {
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.bucketMode = bucketMode;
    }

    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> input) {
        List<DataFileMeta> files = new ArrayList<>(input);
        files.sort(fileComparator(bucketMode == BucketMode.BUCKET_UNAWARE));
        Function<DataFileMeta, Long> weightFunc = file -> Math.max(file.fileSize(), openFileCost);
        return BinPacking.packForOrdered(files, weightFunc, targetSplitSize).stream()
                .map(SplitGroup::rawConvertibleGroup)
                .collect(Collectors.toList());
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        // When the bucket mode is unaware, we spit the files as batch, because unaware-bucket table
        // only contains one bucket (bucket 0).
        if (bucketMode == BucketMode.BUCKET_UNAWARE) {
            return splitForBatch(files);
        } else {
            return Collections.singletonList(SplitGroup.rawConvertibleGroup(files));
        }
    }
}
