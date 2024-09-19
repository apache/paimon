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

package org.apache.paimon.spark.sort;

import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

import static org.apache.spark.sql.functions.array;

/** Sort table by hilbert curve. */
public class HilbertSorter extends TableSorter {

    private static final String H_COLUMN = "HVALUE";

    public HilbertSorter(FileStoreTable table, List<String> orderColumns) {
        super(table, orderColumns);
        checkNotEmpty();
    }

    @Override
    public Dataset<Row> sort(Dataset<Row> df) {
        Column hilbertColumn = hilbertValue(df);
        Dataset<Row> hilbertValueDF = df.withColumn(H_COLUMN, hilbertColumn);
        Dataset<Row> sortedDF =
                hilbertValueDF
                        .repartitionByRange(hilbertValueDF.col(H_COLUMN))
                        .sortWithinPartitions(hilbertValueDF.col(H_COLUMN));
        return sortedDF.drop(H_COLUMN);
    }

    private Column hilbertValue(Dataset<Row> df) {
        SparkHilbertUDF hilbertUDF = new SparkHilbertUDF();

        Column[] hilbertCols =
                orderColNames.stream()
                        .map(df.schema()::apply)
                        .map(
                                col ->
                                        hilbertUDF.sortedLexicographically(
                                                df.col(col.name()), col.dataType()))
                        .toArray(Column[]::new);

        return hilbertUDF.transform(array(hilbertCols));
    }
}
