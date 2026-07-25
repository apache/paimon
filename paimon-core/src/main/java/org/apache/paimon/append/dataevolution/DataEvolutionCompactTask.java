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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Comparator.comparingLong;
import static org.apache.paimon.utils.DataEvolutionUtils.checkContiguousRowRange;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Base class for data evolution table compaction tasks. */
public abstract class DataEvolutionCompactTask {

    protected static final Map<String, String> DYNAMIC_WRITE_OPTIONS = dynamicWriteOptions();
    protected static final Map<String, String> BLOB_COMPACT_READ_OPTIONS =
            Collections.singletonMap(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true");

    protected final BinaryRow partition;
    protected final List<DataFileMeta> compactBefore;
    protected final List<DataFileMeta> compactAfter;

    protected DataEvolutionCompactTask(BinaryRow partition, List<DataFileMeta> files) {
        checkArgument(files != null);
        this.partition = partition;
        this.compactBefore = new ArrayList<>(files);
        this.compactAfter = new ArrayList<>();
    }

    private static Map<String, String> dynamicWriteOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "99999 G");
        options.put(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "99999 G");
        // Data evolution requires a single output file, so the row limit must not roll it either.
        options.put(CoreOptions.TARGET_FILE_ROW_NUM.key(), String.valueOf(Long.MAX_VALUE));
        return Collections.unmodifiableMap(options);
    }

    public BinaryRow partition() {
        return partition;
    }

    public List<DataFileMeta> compactBefore() {
        return compactBefore;
    }

    public List<DataFileMeta> compactAfter() {
        return compactAfter;
    }

    public abstract TaskType type();

    public abstract CommitMessage doCompact(FileStoreTable table, String commitUser)
            throws Exception;

    protected CommitMessage commitMessage(
            List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        CompactIncrement compactIncrement =
                new CompactIncrement(compactBefore, compactAfter, Collections.emptyList());
        return new CommitMessageImpl(
                partition, 0, null, DataIncrement.emptyIncrement(), compactIncrement);
    }

    protected List<DataFileMeta> sortedByFirstRowId(List<DataFileMeta> files) {
        List<DataFileMeta> sorted = new ArrayList<>(files);
        sorted.sort(comparingLong(DataFileMeta::nonNullFirstRowId));
        return sorted;
    }

    protected void checkSameRowRange(
            String fileDescription,
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter) {
        Range beforeRange = checkContiguousRowRange(compactBefore);
        Range afterRange = checkContiguousRowRange(compactAfter);
        checkArgument(
                beforeRange.equals(afterRange),
                "%s compact after files should have the same row range as compact before files, "
                        + "before range is %s, but after range is %s.",
                fileDescription,
                beforeRange,
                afterRange);
    }

    protected long minSequenceId(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(DataFileMeta::minSequenceNumber)
                .min()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Cannot get min sequence id from compact before files."));
    }

    protected long maxSequenceId(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(DataFileMeta::maxSequenceNumber)
                .max()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Cannot get max sequence id from compact before files."));
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, compactBefore, compactAfter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataEvolutionCompactTask that = (DataEvolutionCompactTask) o;
        return Objects.equals(partition, that.partition)
                && Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter);
    }

    @Override
    public String toString() {
        return String.format(
                "%s {partition = %s, compactBefore = %s, compactAfter = %s}",
                getClass().getSimpleName(), partition, compactBefore, compactAfter);
    }

    /** Type of data evolution compaction task. */
    public enum TaskType {
        NORMAL(0),
        BLOB(1),
        MATERIALIZE_DELETION(2);

        private final int code;

        TaskType(int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }

        public static TaskType fromCode(int code) {
            for (TaskType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new UnsupportedOperationException(
                    "Unsupported data evolution compact task type code: " + code);
        }
    }
}
