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

package org.apache.paimon.hive.mapred;

import org.apache.paimon.hive.RowDataContainer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.paimon.hive.utils.HiveUtils.createFileStoreTable;
import static org.apache.paimon.hive.utils.HiveUtils.createPredicate;

/**
 * {@link InputFormat} for paimon. It divides all files into {@link InputSplit}s (one split per
 * bucket) and creates {@link RecordReader} for each split.
 */
public class PaimonInputFormat implements InputFormat<Void, RowDataContainer> {

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) {
        FileStoreTable table = createFileStoreTable(jobConf);
        InnerTableScan scan = table.newScan();
        createPredicate(table.schema(), jobConf).ifPresent(scan::withFilter);
        // If the path of the Paimon table is moved from the location of the Hive table to
        // properties,Hive will add a location for this table based on the warehouse ,
        // database,and table automatically.When querying by Hive, an exception may occur
        // because the specified path for split for Paimon may not match the location of Hive.
        // To work around this problem,we specify the path for split as the location of Hive.
        String location = jobConf.get(hive_metastoreConstants.META_TABLE_LOCATION);
        return scan.plan().splits().stream()
                .map(split -> new PaimonInputSplit(location, (DataSplit) split))
                .toArray(PaimonInputSplit[]::new);
    }

    @Override
    public RecordReader<Void, RowDataContainer> getRecordReader(
            InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        FileStoreTable table = createFileStoreTable(jobConf);
        PaimonInputSplit split = (PaimonInputSplit) inputSplit;
        ReadBuilder readBuilder = table.newReadBuilder();
        createPredicate(table.schema(), jobConf).ifPresent(readBuilder::withFilter);
        return new PaimonRecordReader(
                readBuilder,
                split,
                table.schema().fieldNames(),
                Arrays.asList(getSelectedColumns(jobConf)));
    }

    private String[] getSelectedColumns(JobConf jobConf) {
        // when using tez engine or when same table is joined multiple times,
        // it is possible that some selected columns are duplicated
        return Arrays.stream(ColumnProjectionUtils.getReadColumnNames(jobConf))
                .distinct()
                .toArray(String[]::new);
    }
}
