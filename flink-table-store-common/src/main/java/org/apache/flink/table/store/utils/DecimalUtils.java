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

package org.apache.flink.table.store.utils;

import org.apache.flink.table.data.DecimalData;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.apache.flink.table.data.DecimalData.fromBigDecimal;

/** Utilities for {@link DecimalData}. */
public class DecimalUtils {

    static final int MAX_COMPACT_PRECISION = 18;

    static final long[] POW10 = new long[MAX_COMPACT_PRECISION + 1];

    static {
        POW10[0] = 1;
        for (int i = 1; i < POW10.length; i++) {
            POW10[i] = 10 * POW10[i - 1];
        }
    }

    public static double doubleValue(DecimalData decimal) {
        if (decimal.isCompact()) {
            return ((double) decimal.toUnscaledLong()) / POW10[decimal.scale()];
        } else {
            return decimal.toBigDecimal().doubleValue();
        }
    }

    public static DecimalData add(DecimalData v1, DecimalData v2, int precision, int scale) {
        if (v1.isCompact()
                && v2.isCompact()
                && v1.scale() == v2.scale()
                && DecimalData.isCompact(precision)) {
            assert scale == v1.scale(); // no need to rescale
            try {
                long ls =
                        Math.addExact(v1.toUnscaledLong(), v2.toUnscaledLong()); // checks overflow
                return DecimalData.fromUnscaledLong(ls, precision, scale);
            } catch (ArithmeticException e) {
                // overflow, fall through
            }
        }
        BigDecimal bd = v1.toBigDecimal().add(v2.toBigDecimal());
        return fromBigDecimal(bd, precision, scale);
    }

    public static long castToIntegral(DecimalData dec) {
        BigDecimal bd = dec.toBigDecimal();
        // rounding down. This is consistent with float=>int,
        // and consistent with SQLServer, Spark.
        bd = bd.setScale(0, RoundingMode.DOWN);
        return bd.longValue();
    }

    public static DecimalData castToDecimal(DecimalData dec, int precision, int scale) {
        return fromBigDecimal(dec.toBigDecimal(), precision, scale);
    }

    public static DecimalData castFrom(DecimalData dec, int precision, int scale) {
        return fromBigDecimal(dec.toBigDecimal(), precision, scale);
    }

    public static DecimalData castFrom(String string, int precision, int scale) {
        return fromBigDecimal(new BigDecimal(string), precision, scale);
    }

    public static DecimalData castFrom(double val, int p, int s) {
        return fromBigDecimal(BigDecimal.valueOf(val), p, s);
    }

    public static DecimalData castFrom(long val, int p, int s) {
        return fromBigDecimal(BigDecimal.valueOf(val), p, s);
    }

    public static boolean castToBoolean(DecimalData dec) {
        return dec.toBigDecimal().compareTo(BigDecimal.ZERO) != 0;
    }
}
