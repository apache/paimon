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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.TypeCheckUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Complete file groups selected for a range sort in one table partition. */
public class DataEvolutionRangeSortPlan implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Pattern ROW_ID_RANGE =
            Pattern.compile("\\s*\\[\\s*([0-9]+)\\s*,\\s*([0-9]+)\\s*\\]\\s*");

    private final List<DataSplit> splits;

    private DataEvolutionRangeSortPlan(List<DataSplit> splits) {
        this.splits = splits;
    }

    public static Range parseRange(String value) {
        checkArgument(value != null, "The range argument is required.");
        Matcher matcher = ROW_ID_RANGE.matcher(value);
        checkArgument(
                matcher.matches(),
                "Expected an inclusive row ID range '[start, end]', got '%s'.",
                value);
        long start;
        long end;
        try {
            start = Long.parseLong(matcher.group(1));
            end = Long.parseLong(matcher.group(2));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Row ID range bounds must fit in a BIGINT: " + value, e);
        }
        checkArgument(start <= end, "Invalid inclusive row ID range [%s, %s].", start, end);
        return new Range(start, end);
    }

    public static List<String> parseSortFields(String orderBy) {
        return Arrays.stream(orderBy.split(",", -1)).map(String::trim).collect(Collectors.toList());
    }

    public static void validate(FileStoreTable table, Range range, List<String> fields) {
        checkArgument(
                table.coreOptions().dataEvolutionEnabled(),
                "Range sorting requires a data evolution table.");
        checkArgument(
                !table.coreOptions().queryAuthEnabled(),
                "Range sorting does not support tables with query auth enabled.");
        checkArgument(
                range.from >= 0 && range.from <= range.to,
                "Invalid inclusive row ID range %s.",
                range);
        checkArgument(
                !fields.isEmpty() && new HashSet<>(fields).size() == fields.size(),
                "Specify distinct sort fields.");
        RowType rowType = table.rowType();
        for (String field : fields) {
            int index = rowType.getFieldIndex(field);
            checkArgument(
                    index >= 0 && TypeCheckUtils.isComparable(rowType.getTypeAt(index)),
                    "Expected a comparable top-level sort field, found '%s'.",
                    field);
        }
    }

    public static List<DataEvolutionRangeSortPlan> plan(
            FileStoreTable table,
            Snapshot snapshot,
            Range range,
            @Nullable PartitionPredicate partitionFilter,
            List<String> sortFields) {
        validate(table, range, sortFields);
        checkArgument(
                table.schema().id() >= snapshot.schemaId(),
                "Table schema %s is older than the schema of planning snapshot %s (%s). Reload the table before sorting.",
                table.schema().id(),
                snapshot.id(),
                snapshot.schemaId());
        List<DataSplit> splits =
                table.newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withPartitionFilter(partitionFilter)
                        .withRowRanges(Collections.singletonList(range))
                        .read()
                        .dataSplits();
        Map<BinaryRow, List<DataSplit>> partitions = new LinkedHashMap<>();
        for (DataSplit split : splits) {
            for (DataFileMeta file : split.dataFiles()) {
                Range actual = file.nonNullRowIdRange();
                checkArgument(
                        range.from <= actual.from && actual.to <= range.to,
                        "Row ID range %s cuts file %s covering %s. Select complete file groups.",
                        range,
                        file.fileName(),
                        actual);
            }
            partitions.computeIfAbsent(split.partition(), ignored -> new ArrayList<>()).add(split);
        }
        return partitions.values().stream()
                .map(DataEvolutionRangeSortPlan::new)
                .collect(Collectors.toList());
    }

    /** Split each scan input at normal-file row ID ranges, keeping overlapping column files. */
    public List<IndexedSplit> readSplits() {
        return splits.stream()
                .flatMap(
                        split ->
                                DataEvolutionMaterializeDeletionCompactTask.readSplits(split)
                                        .stream())
                .collect(Collectors.toList());
    }

    public CommitMessage inputFiles() {
        List<DataFileMeta> files =
                splits.stream()
                        .flatMap(split -> split.dataFiles().stream())
                        .collect(Collectors.toList());
        return DataEvolutionRangeShuffle.replacement(
                splits.get(0).partition(), files, Collections.emptyList());
    }
}
