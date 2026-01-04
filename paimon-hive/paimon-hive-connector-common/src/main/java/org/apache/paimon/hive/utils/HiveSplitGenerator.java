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

package org.apache.paimon.hive.utils;

import org.apache.paimon.hive.HiveConnectorOptions;
import org.apache.paimon.hive.mapred.PaimonInputSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.tag.TagPreview;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BinPacking;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.apache.paimon.CoreOptions.SCAN_TAG_NAME;
import static org.apache.paimon.hive.utils.HiveUtils.createPredicate;
import static org.apache.paimon.hive.utils.HiveUtils.extractTagName;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;

/** Generator to generate hive input splits. */
public class HiveSplitGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(HiveSplitGenerator.class);

    public static InputSplit[] generateSplits(
            FileStoreTable table, JobConf jobConf, int numSplits) {
        List<Predicate> predicates = new ArrayList<>();
        createPredicate(table.schema(), jobConf, false).ifPresent(predicates::add);

        // If the path of the Paimon table is moved from the location of the Hive table to
        // properties (see HiveCatalogOptions#LOCATION_IN_PROPERTIES), Hive will add a location for
        // this table based on the warehouse, database, and table automatically. When querying by
        // Hive, an exception may occur because the specified path for split for Paimon may not
        // match the location of Hive. To work around this problem, we specify the path for split as
        // the location of Hive.
        String locations = jobConf.get(FileInputFormat.INPUT_DIR);

        @Nullable String tagToPartField = table.coreOptions().tagToPartitionField();
        @Nullable TagPreview tagPreview = TagPreview.create(table.coreOptions());

        List<PaimonInputSplit> splits = new ArrayList<>();
        // locations may contain multiple partitions
        for (String location : locations.split(",")) {
            if (!location.startsWith(table.location().toUri().toString())) {
                // Hive create dummy file for empty table or partition. If this location doesn't
                // belong to this table, nothing to do.
                continue;
            }

            InnerTableScan scan;
            if (tagToPartField != null) {
                // the location should be pruned by partition predicate
                // we can extract tag name from location, and use time travel to scan
                String tag = extractTagName(location, tagToPartField);
                Map<String, String> dynamicOptions =
                        tagPreview == null
                                ? singletonMap(SCAN_TAG_NAME.key(), tag)
                                : tagPreview.timeTravel(table, tag);
                scan = table.copy(dynamicOptions).newScan();
                if (predicates.size() > 0) {
                    scan.withFilter(PredicateBuilder.and(predicates));
                }
            } else {
                List<Predicate> predicatePerPartition = new ArrayList<>(predicates);
                createPartitionPredicate(
                                table.schema().logicalRowType(),
                                table.schema().partitionKeys(),
                                location,
                                table.coreOptions().partitionDefaultName())
                        .ifPresent(predicatePerPartition::add);

                scan = table.newScan();
                if (predicatePerPartition.size() > 0) {
                    scan.withFilter(PredicateBuilder.and(predicatePerPartition));
                }
            }
            List<DataSplit> dataSplits =
                    scan.dropStats().plan().splits().stream()
                            .map(s -> (DataSplit) s)
                            .collect(Collectors.toList());
            List<DataSplit> packed = dataSplits;
            if (jobConf.getBoolean(
                    HiveConnectorOptions.HIVE_PAIMON_RESPECT_MINMAXSPLITSIZE_ENABLED.key(),
                    false)) {
                packed = packSplits(table, jobConf, dataSplits, numSplits);
            }
            packed.forEach(ss -> splits.add(new PaimonInputSplit(location, ss, table)));
        }
        return splits.toArray(new InputSplit[0]);
    }

    private static Optional<Predicate> createPartitionPredicate(
            RowType rowType,
            List<String> partitionKeys,
            String partitionDir,
            String defaultPartName) {
        Set<String> partitionKeySet = new HashSet<>(partitionKeys);
        LinkedHashMap<String, String> partition = new LinkedHashMap<>();
        for (String s : partitionDir.split("/")) {
            s = s.trim();
            if (s.isEmpty()) {
                continue;
            }
            String[] kv = s.split("=");
            if (kv.length != 2) {
                continue;
            }
            if (partitionKeySet.contains(kv[0])) {
                partition.put(kv[0], kv[1]);
            }
        }
        if (partition.isEmpty() || partition.size() != partitionKeys.size()) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(
                    PartitionPredicate.createPartitionPredicate(
                            partition, rowType, defaultPartName));
        }
    }

    public static List<DataSplit> packSplits(
            FileStoreTable table, JobConf jobConf, List<DataSplit> splits, int numSplits) {
        if (table.coreOptions().deletionVectorsEnabled()) {
            return splits;
        }
        long openCostInBytes =
                jobConf.getLong(
                        HiveConnectorOptions.HIVE_PAIMON_SPLIT_OPENFILECOST.key(),
                        table.coreOptions().splitOpenFileCost());
        long splitSize = computeSplitSize(jobConf, splits, numSplits, openCostInBytes);
        List<DataSplit> dataSplits = new ArrayList<>();
        List<DataSplit> toPack = new ArrayList<>();
        int numFiles = 0;
        for (DataSplit split : splits) {
            if (split instanceof FallbackReadFileStoreTable.FallbackSplit) {
                dataSplits.add(split);
            } else if (split.beforeFiles().isEmpty() && split.rawConvertible()) {
                numFiles += split.dataFiles().size();
                toPack.add(split);
            } else {
                dataSplits.add(split);
            }
        }
        Function<DataFileMeta, Long> weightFunc =
                file -> Math.max(file.fileSize(), openCostInBytes);
        DataSplit current = null;
        List<DataFileMeta> bin = new ArrayList<>();
        int numFilesAfterPacked = 0;
        for (DataSplit split : toPack) {
            if (current == null
                    || (current.partition().equals(split.partition())
                            && current.bucket() == split.bucket())) {
                current = split;
                bin.addAll(split.dataFiles());
            } else {
                // deal with files which belong to the previous partition or bucket.
                List<List<DataFileMeta>> splitGroups =
                        BinPacking.packForOrdered(bin, weightFunc, splitSize);
                for (List<DataFileMeta> fileGroups : splitGroups) {
                    DataSplit newSplit = buildDataSplit(current, fileGroups);
                    numFilesAfterPacked += newSplit.dataFiles().size();
                    dataSplits.add(newSplit);
                }
                bin.clear();
                current = split;
                bin.addAll(split.dataFiles());
            }
        }
        if (!bin.isEmpty()) {
            List<List<DataFileMeta>> splitGroups =
                    BinPacking.packForOrdered(bin, weightFunc, splitSize);
            for (List<DataFileMeta> fileGroups : splitGroups) {
                DataSplit newSplit = buildDataSplit(current, fileGroups);
                numFilesAfterPacked += newSplit.dataFiles().size();
                dataSplits.add(newSplit);
            }
        }
        LOG.info("The origin number of data files before pack: {}", numFiles);
        LOG.info("The current number of data files after pack: {}", numFilesAfterPacked);
        return dataSplits;
    }

    private static DataSplit buildDataSplit(DataSplit current, List<DataFileMeta> fileGroups) {
        return DataSplit.builder()
                .withSnapshot(current.snapshotId())
                .withPartition(current.partition())
                .withBucket(current.bucket())
                .withTotalBuckets(current.totalBuckets())
                .withDataFiles(fileGroups)
                .rawConvertible(current.rawConvertible())
                .withBucketPath(current.bucketPath())
                .build();
    }

    private static Long computeSplitSize(
            JobConf jobConf, List<DataSplit> splits, int numSplits, long openCostInBytes) {
        long maxSize = HiveConf.getLongVar(jobConf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE);
        long minSize = HiveConf.getLongVar(jobConf, HiveConf.ConfVars.MAPREDMINSPLITSIZE);
        long avgSize;
        long splitSize;
        if (numSplits > 0) {
            long totalSize = 0;
            for (DataSplit split : splits) {
                totalSize +=
                        split.dataFiles().stream()
                                .map(f -> Math.max(f.fileSize(), openCostInBytes))
                                .reduce(Long::sum)
                                .orElse(0L);
            }
            avgSize = totalSize / numSplits;
            splitSize = Math.min(maxSize, Math.max(avgSize, minSize));
        } else {
            avgSize = 0;
            splitSize = Math.min(maxSize, minSize);
        }
        LOG.info(
                "Currently, minSplitSize: {}, maxSplitSize: {}, avgSize: {}, finalSplitSize: {}.",
                minSize,
                maxSize,
                avgSize,
                splitSize);
        return splitSize;
    }
}
