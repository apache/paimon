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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.MergeTreeSplitGenerator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/** Source selection for PK vector indexes before query predicates or split packing. */
public final class PkVectorSourceFiles {

    private PkVectorSourceFiles() {}

    public static List<DataFileMeta> eligible(
            List<DataFileMeta> bucketFiles, @Nullable CoreOptions options) {
        if (options != null) {
            List<DataFileMeta> visibleFiles =
                    options.batchScanSkipLevel0()
                            ? bucketFiles.stream()
                                    .filter(file -> file.level() > 0)
                                    .collect(Collectors.toList())
                            : bucketFiles;
            if (MergeTreeSplitGenerator.canReadBucketWithoutMerge(
                            visibleFiles, options.deletionVectorsEnabled(), options.mergeEngine())
                    && visibleFiles.stream()
                            .allMatch(
                                    file ->
                                            file.level() > 0
                                                    && file.deleteRowCount().isPresent())) {
                return visibleFiles;
            }
        }
        return bucketFiles.stream()
                .filter(PrimaryKeyIndexSourcePolicy::shouldRead)
                .collect(Collectors.toList());
    }
}
