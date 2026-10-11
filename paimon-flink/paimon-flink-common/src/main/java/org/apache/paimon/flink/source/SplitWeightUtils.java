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

package org.apache.paimon.flink.source;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.SerializableFunction;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utilities for parsing and applying split weight options. */
final class SplitWeightUtils {

    private SplitWeightUtils() {}

    static SerializableFunction<FileStoreSourceSplit, Long> splitWeightFunc(Options options) {
        return splitWeightFunc(
                options, options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE));
    }

    static SerializableFunction<FileStoreSourceSplit, Long> splitWeightFunc(
            Options options, FlinkConnectorOptions.SplitAssignMode splitAssignMode) {
        validateSplitWeightMode(options, splitAssignMode);
        switch (options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_WEIGHT_MODE)) {
            case FILE_SIZE:
                return SplitWeightUtils::splitFileSizeOrRowCount;
            case ROW_COUNT:
                return split -> split.split().rowCount();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported split weight mode "
                                + options.get(
                                        FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_WEIGHT_MODE));
        }
    }

    private static void validateSplitWeightMode(
            Options options, FlinkConnectorOptions.SplitAssignMode splitAssignMode) {
        checkArgument(
                options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_WEIGHT_MODE)
                                != FlinkConnectorOptions.SplitWeightMode.FILE_SIZE
                        || splitAssignMode == FlinkConnectorOptions.SplitAssignMode.FAIR,
                "'%s' = '%s' only works with '%s' = '%s'.",
                FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_WEIGHT_MODE.key(),
                FlinkConnectorOptions.SplitWeightMode.FILE_SIZE,
                FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE.key(),
                FlinkConnectorOptions.SplitAssignMode.FAIR);
    }

    @VisibleForTesting
    static long splitFileSizeOrRowCount(FileStoreSourceSplit sourceSplit) {
        Split split = sourceSplit.split();
        while (split instanceof QueryAuthSplit) {
            split = ((QueryAuthSplit) split).split();
        }
        if (split instanceof DataSplit) {
            return ((DataSplit) split)
                    .dataFiles().stream().mapToLong(file -> file.fileSize()).sum();
        }
        return split.rowCount();
    }
}
