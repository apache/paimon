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

package org.apache.paimon.globalindex;

import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;

/** Builder for scanning global indexes. */
public interface GlobalIndexScanBuilder {

    GlobalIndexScanBuilder withSnapshot(long snapshotId);

    GlobalIndexScanBuilder withSnapshot(Snapshot snapshot);

    GlobalIndexScanBuilder withPartitionPredicate(PartitionPredicate partitionPredicate);

    GlobalIndexScanBuilder withRowRange(Range rowRange);

    RowRangeGlobalIndexScanner build();

    // Return sorted and no overlap ranges
    List<Range> shardList();

    static Optional<GlobalIndexResult> parallelScan(
            final List<Range> ranges,
            final GlobalIndexScanBuilder globalIndexScanBuilder,
            final Predicate filter,
            @Nullable final VectorSearch vectorSearch,
            final Integer threadNum) {
        List<RowRangeGlobalIndexScanner> scanners =
                ranges.stream()
                        .map(globalIndexScanBuilder::withRowRange)
                        .map(GlobalIndexScanBuilder::build)
                        .collect(Collectors.toList());

        try {
            List<Optional<GlobalIndexResult>> rowsResults = new ArrayList<>();
            Iterator<Optional<GlobalIndexResult>> resultIterators =
                    randomlyExecuteSequentialReturn(
                            scanner -> {
                                Optional<GlobalIndexResult> result =
                                        scanner.scan(filter, vectorSearch);
                                return Collections.singletonList(result);
                            },
                            scanners,
                            threadNum);
            while (resultIterators.hasNext()) {
                rowsResults.add(resultIterators.next());
            }
            if (rowsResults.stream().noneMatch(Optional::isPresent)) {
                return Optional.empty();
            }

            GlobalIndexResult globalIndexResult = GlobalIndexResult.createEmpty();

            for (int i = 0; i < ranges.size(); i++) {
                if (rowsResults.get(i).isPresent()) {
                    globalIndexResult = globalIndexResult.or(rowsResults.get(i).get());
                } else {
                    globalIndexResult =
                            globalIndexResult.or(GlobalIndexResult.fromRange(ranges.get(i)));
                }
            }
            return Optional.of(globalIndexResult);
        } finally {
            IOUtils.closeAllQuietly(scanners);
        }
    }
}
