/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.hive.mapred;

import org.apache.paimon.hive.RowDataContainer;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.hive.utils.HiveUtils.createFileStoreTable;
import static org.apache.paimon.hive.utils.HiveUtils.createPredicate;

/**
 * {@link InputFormat} for paimon. It divides all files into {@link InputSplit}s (one split per
 * bucket) and creates {@link RecordReader} for each split.
 */
public class PaimonInputFormat implements InputFormat<Void, RowDataContainer> {

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) {
        // If the path of the Paimon table is moved from the location of the Hive table to
        // properties, Hive will add a location for this table based on the warehouse,
        // database, and table automatically.When querying by Hive, an exception may occur
        // because the specified path for split for Paimon may not match the location of Hive.
        // To work around this problem, we specify the path for split as the location of Hive.
        String location = jobConf.get(hive_metastoreConstants.META_TABLE_LOCATION);

        FileStoreTable table = createFileStoreTable(jobConf);
        InnerTableScan scan = table.newScan();

        List<Predicate> predicates = new ArrayList<>();
        String inputDir = jobConf.get(FileInputFormat.INPUT_DIR);
        createPartitionPredicate(table.schema().logicalRowType(), location, inputDir)
                .ifPresent(predicates::add);
        createPredicate(table.schema(), jobConf, false).ifPresent(predicates::add);
        if (predicates.size() > 0) {
            scan.withFilter(PredicateBuilder.and(predicates));
        }

        return scan.plan().splits().stream()
                .map(split -> new PaimonInputSplit(location, (DataSplit) split))
                .toArray(PaimonInputSplit[]::new);
    }

    private Optional<Predicate> createPartitionPredicate(
            RowType rowType, String tablePath, String inputDir) {
        while (tablePath.length() > 0 && tablePath.endsWith("/")) {
            tablePath = tablePath.substring(0, tablePath.length() - 1);
        }
        if (inputDir.length() < tablePath.length()) {
            return Optional.empty();
        }

        LinkedHashMap<String, String> partition = new LinkedHashMap<>();
        for (String s : inputDir.substring(tablePath.length()).split("/")) {
            s = s.trim();
            if (s.isEmpty()) {
                continue;
            }
            String[] kv = s.split("=");
            if (kv.length != 2) {
                continue;
            }
            partition.put(kv[0].trim(), kv[1].trim());
        }
        return Optional.ofNullable(PredicateBuilder.partition(partition, rowType));
    }

    @Override
    public RecordReader<Void, RowDataContainer> getRecordReader(
            InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        FileStoreTable table = createFileStoreTable(jobConf);
        PaimonInputSplit split = (PaimonInputSplit) inputSplit;
        ReadBuilder readBuilder = table.newReadBuilder();
        createPredicate(table.schema(), jobConf, true).ifPresent(readBuilder::withFilter);
        List<String> paimonColumns = table.schema().fieldNames();
        return new PaimonRecordReader(
                readBuilder,
                split,
                paimonColumns,
                getSchemaEvolutionColumns(jobConf).orElse(paimonColumns),
                Arrays.asList(getSelectedColumns(jobConf)));
    }

    private Optional<List<String>> getSchemaEvolutionColumns(JobConf jobConf) {
        String columns = jobConf.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS);
        String delimiter =
                jobConf.get(
                        // serdeConstants.COLUMN_NAME_DELIMITER is not defined in earlier Hive
                        // versions, so we use a constant string instead
                        "column.name.delimite", String.valueOf(SerDeUtils.COMMA));
        if (columns == null || delimiter == null) {
            return Optional.empty();
        } else {
            return Optional.of(Arrays.asList(columns.split(delimiter)));
        }
    }

    private String[] getSelectedColumns(JobConf jobConf) {
        // when using tez engine or when same table is joined multiple times,
        // it is possible that some selected columns are duplicated
        return Arrays.stream(ColumnProjectionUtils.getReadColumnNames(jobConf))
                .distinct()
                .toArray(String[]::new);
    }
}
