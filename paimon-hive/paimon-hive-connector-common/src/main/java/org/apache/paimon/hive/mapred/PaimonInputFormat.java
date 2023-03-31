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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.hive.PaimonJobConf;
import org.apache.paimon.hive.RowDataContainer;
import org.apache.paimon.hive.SearchArgumentToPredicateConverter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.ReadBuilder;

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
 * {@link InputFormat} for paimon. It divides all files into {@link InputSplit}s (one split per
 * bucket) and creates {@link RecordReader} for each split.
 */
public class PaimonInputFormat implements InputFormat<Void, RowDataContainer> {

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) {
        FileStoreTable table = createFileStoreTable(jobConf);
        DataTableScan scan = table.newScan();
        createPredicate(table.schema(), jobConf).ifPresent(scan::withFilter);
        return scan.plan().splits.stream()
                .map(split -> new PaimonInputSplit(table.location().toString(), split))
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

    private FileStoreTable createFileStoreTable(JobConf jobConf) {
        PaimonJobConf wrapper = new PaimonJobConf(jobConf);
        Options options = PaimonJobConf.extractCatalogConfig(jobConf);
        options.set(CoreOptions.PATH, wrapper.getLocation());
        CatalogContext catalogContext = CatalogContext.create(options, jobConf);
        return FileStoreTableFactory.create(catalogContext);
    }

    private Optional<Predicate> createPredicate(TableSchema tableSchema, JobConf jobConf) {
        SearchArgument sarg = ConvertAstToSearchArg.createFromConf(jobConf);
        if (sarg == null) {
            return Optional.empty();
        }
        SearchArgumentToPredicateConverter converter =
                new SearchArgumentToPredicateConverter(
                        sarg,
                        tableSchema.fieldNames(),
                        tableSchema.logicalRowType().getFieldTypes());
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
