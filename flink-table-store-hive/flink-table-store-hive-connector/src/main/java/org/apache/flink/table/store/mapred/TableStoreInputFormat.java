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

package org.apache.flink.table.store.mapred;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.RowDataContainer;
import org.apache.flink.table.store.SearchArgumentToPredicateConverter;
import org.apache.flink.table.store.TableStoreJobConf;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.filesystem.FileSystems;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.TableRead;

import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/**
 * {@link InputFormat} for table store. It divides all files into {@link InputSplit}s (one split per
 * bucket) and creates {@link RecordReader} for each split.
 */
public class TableStoreInputFormat implements InputFormat<Void, RowDataContainer> {

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) {
        FileStoreTable table = createFileStoreTable(jobConf);
        DataTableScan scan = table.newScan();
        createPredicate(table.schema(), jobConf).ifPresent(scan::withFilter);
        return scan.plan().splits.stream()
                .map(split -> new TableStoreInputSplit(table.location().toString(), split))
                .toArray(TableStoreInputSplit[]::new);
    }

    @Override
    public RecordReader<Void, RowDataContainer> getRecordReader(
            InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        FileStoreTable table = createFileStoreTable(jobConf);
        TableStoreInputSplit split = (TableStoreInputSplit) inputSplit;
        TableRead read = table.newRead();
        createPredicate(table.schema(), jobConf).ifPresent(read::withFilter);
        return new TableStoreRecordReader(
                read,
                split,
                table.schema().fieldNames(),
                Arrays.asList(getSelectedColumns(jobConf)));
    }

    private FileStoreTable createFileStoreTable(JobConf jobConf) {
        TableStoreJobConf wrapper = new TableStoreJobConf(jobConf);
        Configuration conf = new Configuration();
        conf.set(CoreOptions.PATH, wrapper.getLocation());
        FileSystems.initialize(new Path(wrapper.getLocation()), wrapper.getCatalogConfig());
        return FileStoreTableFactory.create(conf);
    }

    private Optional<Predicate> createPredicate(TableSchema tableSchema, JobConf jobConf) {
        SearchArgument sarg = ConvertAstToSearchArg.createFromConf(jobConf);
        if (sarg == null) {
            return Optional.empty();
        }
        SearchArgumentToPredicateConverter converter =
                new SearchArgumentToPredicateConverter(
                        sarg, tableSchema.fieldNames(), tableSchema.logicalRowType().getChildren());
        return converter.convert();
    }

    private String[] getSelectedColumns(JobConf jobConf) {
        // when using tez engine or when same table is joined multiple times,
        // it is possible that some selected columns are duplicated
        return Arrays.stream(ColumnProjectionUtils.getReadColumnNames(jobConf))
                .distinct()
                .toArray(String[]::new);
    }
}
