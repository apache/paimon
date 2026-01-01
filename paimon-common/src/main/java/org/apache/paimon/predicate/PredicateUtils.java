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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternal;

/** Utilities for working with {@link Predicate} which rely on paimon-common runtime types. */
public class PredicateUtils {

    private PredicateUtils() {}

    public static Object convertJavaObject(DataType literalType, Object o) {
        if (o == null) {
            return null;
        }
        switch (literalType.getTypeRoot()) {
            case BOOLEAN:
                return o;
            case BIGINT:
                return ((Number) o).longValue();
            case DOUBLE:
                return ((Number) o).doubleValue();
            case TINYINT:
                return ((Number) o).byteValue();
            case SMALLINT:
                return ((Number) o).shortValue();
            case INTEGER:
                return ((Number) o).intValue();
            case FLOAT:
                return ((Number) o).floatValue();
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString(o.toString());
            case DATE:
                // Hive uses `java.sql.Date.valueOf(lit.toString());` to convert a literal to Date
                // Which uses `java.util.Date()` internally to create the object and that uses the
                // TimeZone.getDefaultRef()
                // To get back the expected date we have to use the LocalDate which gets rid of the
                // TimeZone misery as it uses the year/month/day to generate the object
                LocalDate localDate;
                if (o instanceof java.sql.Timestamp) {
                    localDate = ((java.sql.Timestamp) o).toLocalDateTime().toLocalDate();
                } else if (o instanceof Date) {
                    localDate = ((Date) o).toLocalDate();
                } else if (o instanceof LocalDate) {
                    localDate = (LocalDate) o;
                } else {
                    throw new UnsupportedOperationException(
                            "Unexpected date literal of class " + o.getClass().getName());
                }
                LocalDate epochDay =
                        Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDate();
                return (int) ChronoUnit.DAYS.between(epochDay, localDate);
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime localTime;
                if (o instanceof java.sql.Time) {
                    localTime = ((java.sql.Time) o).toLocalTime();
                } else if (o instanceof java.time.LocalTime) {
                    localTime = (java.time.LocalTime) o;
                } else {
                    throw new UnsupportedOperationException(
                            "Unexpected time literal of class " + o.getClass().getName());
                }
                // return millis of a day
                return (int) (localTime.toNanoOfDay() / 1_000_000);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) literalType;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return Decimal.fromBigDecimal((BigDecimal) o, precision, scale);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (o instanceof java.sql.Timestamp) {
                    return Timestamp.fromSQLTimestamp((java.sql.Timestamp) o);
                } else if (o instanceof Instant) {
                    Instant o1 = (Instant) o;
                    LocalDateTime dateTime = o1.atZone(ZoneId.systemDefault()).toLocalDateTime();
                    return Timestamp.fromLocalDateTime(dateTime);
                } else if (o instanceof LocalDateTime) {
                    return Timestamp.fromLocalDateTime((LocalDateTime) o);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported class %s for timestamp without timezone ",
                                    o.getClass()));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (o instanceof java.sql.Timestamp) {
                    java.sql.Timestamp timestamp = (java.sql.Timestamp) o;
                    return Timestamp.fromInstant(timestamp.toInstant());
                } else if (o instanceof Instant) {
                    return Timestamp.fromInstant((Instant) o);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported class %s for timestamp with local time zone ",
                                    o.getClass()));
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported predicate leaf type " + literalType.getTypeRoot().name());
        }
    }

    @Nullable
    public static Predicate partition(
            Map<String, String> map, RowType rowType, String defaultPartValue) {
        Map<String, Object> internalValues = convertSpecToInternal(map, rowType, defaultPartValue);
        List<String> fieldNames = rowType.getFieldNames();
        Predicate predicate = null;
        PredicateBuilder builder = new PredicateBuilder(rowType);
        for (Map.Entry<String, Object> entry : internalValues.entrySet()) {
            int idx = fieldNames.indexOf(entry.getKey());
            Object literal = internalValues.get(entry.getKey());
            Predicate predicateTemp =
                    literal == null ? builder.isNull(idx) : builder.equal(idx, literal);
            if (predicate == null) {
                predicate = predicateTemp;
            } else {
                predicate = PredicateBuilder.and(predicate, predicateTemp);
            }
        }
        return predicate;
    }

    public static Predicate partitions(
            List<Map<String, String>> partitions, RowType rowType, String defaultPartValue) {
        return PredicateBuilder.or(
                partitions.stream()
                        .map(p -> PredicateUtils.partition(p, rowType, defaultPartValue))
                        .toArray(Predicate[]::new));
    }
}
