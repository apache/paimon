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
import org.apache.flink.table.store.RowDataContainer;
import org.apache.flink.table.store.SearchArgumentToPredicateConverter;
import org.apache.flink.table.store.TableStoreJobConf;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.source.TableScan;

import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Optional;

/**
 * {@link InputFormat} for table store. It divides all files into {@link InputSplit}s (one split per
 * bucket) and creates {@link RecordReader} for each split.
 */
public class TableStoreInputFormat implements InputFormat<Void, RowDataContainer> {

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        FileStoreTable table = createFileStoreTable(jobConf);
        TableScan scan = table.newScan();
        createPredicate(jobConf).ifPresent(scan::withFilter);
        return scan.plan().splits.stream()
                .map(TableStoreInputSplit::create)
                .toArray(TableStoreInputSplit[]::new);
    }

    @Override
    public RecordReader<Void, RowDataContainer> getRecordReader(
            InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        FileStoreTable table = createFileStoreTable(jobConf);
        TableStoreInputSplit split = (TableStoreInputSplit) inputSplit;
        long splitLength = split.getLength();
        return new TableStoreRecordReader(
                table.newRead().createReader(split.partition(), split.bucket(), split.files()),
                splitLength);
    }

    private FileStoreTable createFileStoreTable(JobConf jobConf) {
        TableStoreJobConf wrapper = new TableStoreJobConf(jobConf);

        String tableLocation = wrapper.getLocation();
        Schema schema =
                new SchemaManager(new Path(tableLocation))
                        .latest()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Schema file not found in location "
                                                        + tableLocation
                                                        + ". Please create table first."));
        Configuration conf = new Configuration();
        wrapper.updateFileStoreOptions(conf);
        return FileStoreTable.create(schema, false, conf, wrapper.getFileStoreUser());
    }

    private Optional<Predicate> createPredicate(JobConf jobConf) {
        String hiveFilter = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if (hiveFilter == null) {
            return Optional.empty();
        }

        TableStoreJobConf wrapper = new TableStoreJobConf(jobConf);
        ExprNodeGenericFuncDesc exprNodeDesc =
                SerializationUtilities.deserializeObject(hiveFilter, ExprNodeGenericFuncDesc.class);
        SearchArgument sarg = ConvertAstToSearchArg.create(jobConf, exprNodeDesc);
        SearchArgumentToPredicateConverter converter =
                new SearchArgumentToPredicateConverter(
                        sarg, wrapper.getColumnNames(), wrapper.getColumnTypes());
        return converter.convert();
    }
}
