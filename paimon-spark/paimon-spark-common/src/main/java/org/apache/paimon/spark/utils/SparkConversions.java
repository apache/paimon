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

package org.apache.paimon.spark.utils;

import org.apache.paimon.utils.Preconditions;

import org.apache.spark.sql.types.Decimal;

import java.util.function.Function;

public class SparkConversions {
    private SparkConversions() {}

    public static Function<Decimal, org.apache.paimon.data.Decimal> decimalSparkToPaimon(
            int precision, int scale) {
        Preconditions.checkArgument(
                precision <= 38,
                "Decimals with precision larger than 38 are not supported: %s",
                precision);

        if (org.apache.paimon.data.Decimal.isCompact(precision)) {
            return sparkDecimal ->
                    org.apache.paimon.data.Decimal.fromUnscaledLong(
                            sparkDecimal.toUnscaledLong(), precision, scale);
        } else {
            return sparkDecimal ->
                    org.apache.paimon.data.Decimal.fromBigDecimal(
                            sparkDecimal.toJavaBigDecimal(), precision, scale);
        }
    }
}
