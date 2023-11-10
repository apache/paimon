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

/** Sort table by zorder. */
public class ZorderSorter extends TableSorter {

    private static final String Z_COLUMN = "ZVALUE";

    public ZorderSorter(FileStoreTable table, List<String> zOrderColNames) {
        super(table, zOrderColNames);
        checkNotEmpty();
    }

    public Dataset<Row> sort(Dataset<Row> df) {
        Column zColumn = zValue(df);
        Dataset<Row> zValueDF = df.withColumn(Z_COLUMN, zColumn);
        Dataset<Row> sortedDF =
                zValueDF.repartitionByRange(zValueDF.col(Z_COLUMN))
                        .sortWithinPartitions(zValueDF.col(Z_COLUMN));
        return sortedDF.drop(Z_COLUMN);
    }

    private Column zValue(Dataset<Row> df) {
        SparkZOrderUDF zOrderUDF =
                new SparkZOrderUDF(
                        orderColNames.size(),
                        table.store().options().varTypeSize(),
                        Integer.MAX_VALUE);

        Column[] zOrderCols =
                orderColNames.stream()
                        .map(df.schema()::apply)
                        .map(
                                col ->
                                        zOrderUDF.sortedLexicographically(
                                                df.col(col.name()), col.dataType()))
                        .toArray(Column[]::new);

        return zOrderUDF.interleaveBytes(array(zOrderCols));
    }
}
