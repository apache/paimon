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

/** Order sort by alphabetical order. */
public class OrderSorter extends TableSorter {

    public OrderSorter(FileStoreTable table, List<String> orderColNames) {
        super(table, orderColNames);
        checkNotEmpty();
    }

    @Override
    public Dataset<Row> sort(Dataset<Row> input) {
        Column[] sortColumns = orderColNames.stream().map(input::col).toArray(Column[]::new);
        return input.repartitionByRange(sortColumns).sortWithinPartitions(sortColumns);
    }
}
