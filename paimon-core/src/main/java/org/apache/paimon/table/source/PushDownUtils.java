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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.paimon.utils.ListUtils.isNullOrEmpty;

/** Utils for pushing downs. */
public class PushDownUtils {

    public static boolean minmaxAvailable(DataType type) {
        // not push down complex type
        // not push down Timestamp because INT96 sort order is undefined,
        // Parquet doesn't return statistics for INT96
        // not push down Parquet Binary because min/max could be truncated
        // (https://issues.apache.org/jira/browse/PARQUET-1685), Parquet Binary
        // could be Spark StringType, BinaryType or DecimalType.
        // not push down for ORC with same reason.
        return type instanceof BooleanType
                || type instanceof TinyIntType
                || type instanceof SmallIntType
                || type instanceof IntType
                || type instanceof BigIntType
                || type instanceof FloatType
                || type instanceof DoubleType
                || type instanceof DateType;
    }

    public static boolean minmaxAvailable(Split split, Set<String> columns) {
        if (!(split instanceof DataSplit)) {
            return false;
        }

        DataSplit dataSplit = (DataSplit) split;
        if (isNullOrEmpty(columns)) {
            return false;
        }

        if (!dataSplit.rawConvertible()) {
            return false;
        }

        return dataSplit.dataFiles().stream()
                .map(DataFileMeta::valueStatsCols)
                .allMatch(
                        valueStatsCols ->
                                // It means there are all column statistics when valueStatsCols ==
                                // null
                                valueStatsCols == null
                                        || new HashSet<>(valueStatsCols).containsAll(columns));
    }
}
