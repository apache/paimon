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

package org.apache.flink.table.store.file.predicate;

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/** A serializable literal class. */
public class Literal implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final BytePrimitiveArrayComparator BINARY_COMPARATOR =
            new BytePrimitiveArrayComparator(true);

    private final LogicalType type;

    private transient Object value;

    public Literal(LogicalType type, Object value) {
        this.type = type;
        this.value = value;
    }

    public LogicalType type() {
        return type;
    }

    public Object value() {
        return value;
    }

    public int compareValueTo(Object o) {
        if (value instanceof Comparable) {
            return ((Comparable<Object>) value).compareTo(o);
        } else if (value instanceof byte[]) {
            return BINARY_COMPARATOR.compare((byte[]) value, (byte[]) o);
        } else {
            throw new RuntimeException("Unsupported type: " + type);
        }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        InternalSerializers.create(type).serialize(value, new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        value = InternalSerializers.create(type).deserialize(new DataInputViewStreamWrapper(in));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Literal)) {
            return false;
        }
        Literal literal = (Literal) o;
        return type.equals(literal.type) && value.equals(literal.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    public static Literal fromJavaObject(LogicalType literalType, Object o) {
        if (o == null) {
            throw new UnsupportedOperationException("Null literals are currently unsupported");
        }
        switch (literalType.getTypeRoot()) {
            case BOOLEAN:
                return new Literal(literalType, o);
            case BIGINT:
                return new Literal(literalType, ((Number) o).longValue());
            case DOUBLE:
                return new Literal(literalType, ((Number) o).doubleValue());
            case TINYINT:
                return new Literal(literalType, ((Number) o).byteValue());
            case SMALLINT:
                return new Literal(literalType, ((Number) o).shortValue());
            case INTEGER:
                return new Literal(literalType, ((Number) o).intValue());
            case FLOAT:
                return new Literal(literalType, ((Number) o).floatValue());
            case VARCHAR:
                return new Literal(literalType, StringData.fromString(o.toString()));
            case DATE:
                // Hive uses `java.sql.Date.valueOf(lit.toString());` to convert a literal to Date
                // Which uses `java.util.Date()` internally to create the object and that uses the
                // TimeZone.getDefaultRef()
                // To get back the expected date we have to use the LocalDate which gets rid of the
                // TimeZone misery as it uses the year/month/day to generate the object
                LocalDate localDate;
                if (o instanceof Timestamp) {
                    localDate = ((Timestamp) o).toLocalDateTime().toLocalDate();
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
                int numberOfDays = (int) ChronoUnit.DAYS.between(epochDay, localDate);
                return new Literal(literalType, numberOfDays);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) literalType;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return new Literal(
                        literalType, DecimalData.fromBigDecimal((BigDecimal) o, precision, scale));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampData timestampData;
                if (o instanceof Timestamp) {
                    timestampData = TimestampData.fromTimestamp((Timestamp) o);
                } else if (o instanceof Instant) {
                    timestampData = TimestampData.fromInstant((Instant) o);
                } else if (o instanceof LocalDateTime) {
                    timestampData = TimestampData.fromLocalDateTime((LocalDateTime) o);
                } else {
                    throw new UnsupportedOperationException("Unsupported object: " + o);
                }
                return new Literal(literalType, timestampData);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported predicate leaf type " + literalType.getTypeRoot().name());
        }
    }
}
