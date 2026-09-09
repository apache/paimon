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

package org.apache.paimon.operation.commit;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.RowRangeIndex;

import java.util.List;
import java.util.stream.Collectors;

/** Detects row ID conflicts solely by row range overlap. */
public class RowIdRangeConflictChecker implements RowIdConflictChecker {

    private final RowRangeIndex rowRangeIndex;

    private RowIdRangeConflictChecker(List<DataFileMeta> deltaFiles) {
        this.rowRangeIndex =
                RowRangeIndex.create(
                        deltaFiles.stream()
                                .filter(file -> file.firstRowId() != null)
                                .map(DataFileMeta::nonNullRowIdRange)
                                .collect(Collectors.toList()));
    }

    public static RowIdRangeConflictChecker fromDataFiles(List<DataFileMeta> deltaFiles) {
        return new RowIdRangeConflictChecker(deltaFiles);
    }

    @Override
    public boolean isEmpty() {
        return rowRangeIndex.ranges().isEmpty();
    }

    @Override
    public boolean conflictsWith(DataFileMeta file) {
        return file.firstRowId() != null
                && rowRangeIndex.intersects(
                        file.nonNullRowIdRange().from, file.nonNullRowIdRange().to);
    }
}
