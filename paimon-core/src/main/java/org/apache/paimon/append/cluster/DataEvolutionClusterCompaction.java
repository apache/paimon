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

package org.apache.paimon.append.cluster;

import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionNormalCompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.RangeHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compacts column versions left outside the incremental clustering selection. */
final class DataEvolutionClusterCompaction {

    private DataEvolutionClusterCompaction() {}

    static List<DataEvolutionCompactTask> plan(
            FileStoreTable table,
            DataEvolutionIncrementalClusterRewriter rewriter,
            Map<BinaryRow, List<DataFileMeta>> partitions,
            Map<BinaryRow, CompactUnit> clusteringUnits) {
        RangeHelper<DataFileMeta> helper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        List<DataEvolutionCompactTask> tasks = new ArrayList<>();
        partitions.forEach(
                (partition, files) -> {
                    CompactUnit clustering = clusteringUnits.get(partition);
                    Set<String> selected =
                            clustering == null
                                    ? Collections.emptySet()
                                    : clustering.files().stream()
                                            .map(DataFileMeta::fileName)
                                            .collect(Collectors.toSet());
                    List<DataFileMeta> normalFiles =
                            files.stream()
                                    .filter(
                                            file ->
                                                    !isBlobFile(file.fileName())
                                                            && !isVectorStoreFile(file.fileName()))
                                    .collect(Collectors.toList());
                    for (List<DataFileMeta> group : helper.mergeOverlappingRanges(normalFiles)) {
                        checkArgument(
                                helper.areAllRangesSame(group),
                                "Normal files must have equal row-id ranges.");
                        if (group.size() < Math.max(2, table.coreOptions().compactionMinFileNum())
                                || group.stream()
                                        .anyMatch(file -> selected.contains(file.fileName()))) {
                            continue;
                        }
                        // A single group preserves even LOCAL_SORT order when its level is trusted.
                        // Dirty groups still compact at L0 without claiming that they are sorted.
                        tasks.add(
                                new DataEvolutionNormalCompactTask(
                                        partition,
                                        group,
                                        Collections.emptyList(),
                                        rewriter.groupLevel(group)));
                    }
                });
        return tasks;
    }
}
