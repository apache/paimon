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

package org.apache.paimon.types;

import org.apache.paimon.data.Decimal;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Table level col stats, supports the following stats.
 *
 * <ul>
 *   <li>distinctCount: distinct count
 *   <li>min: min value
 *   <li>max: max value
 *   <li>nullCount: null count
 *   <li>avgLen: average length
 *   <li>maxLen: max length
 * </ul>
 */
public class DataFieldStats implements Serializable {
    private static final long serialVersionUID = 1L;
    private final @Nullable Long distinctCount;
    private final @Nullable Object min;
    private final @Nullable Object max;
    private final @Nullable Long nullCount;
    private final @Nullable Long avgLen;
    private final @Nullable Long maxLen;

    public DataFieldStats(
            @Nullable Long distinctCount,
            @Nullable Object min,
            @Nullable Object max,
            @Nullable Long nullCount,
            @Nullable Long avgLen,
            @Nullable Long maxLen) {
        this.distinctCount = distinctCount;
        this.min = min;
        this.max = max;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
    }

    public @Nullable Long distinctCount() {
        return distinctCount;
    }

    public @Nullable Object min() {
        return min;
    }

    public @Nullable Object max() {
        return max;
    }

    public @Nullable Long nullCount() {
        return nullCount;
    }

    public @Nullable Long avgLen() {
        return avgLen;
    }

    public @Nullable Long maxLen() {
        return maxLen;
    }

    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        if (distinctCount != null) {
            generator.writeNumberField("distinctCount", distinctCount);
        }
        // todo: just for quick test, need to find a more reasonable serialization method
        if (min != null) {
            Object o = min;
            if (o instanceof String) {
                generator.writeStringField("min", (String) o);
            } else if (o instanceof Long) {
                generator.writeNumberField("min", (Long) o);
            } else if (o instanceof Integer) {
                generator.writeNumberField("min", (Integer) o);
            } else if (o instanceof Double) {
                generator.writeNumberField("min", (Double) o);
            } else if (o instanceof Float) {
                generator.writeNumberField("min", (Float) o);
            } else if (o instanceof Boolean) {
                generator.writeBooleanField("min", (Boolean) o);
            } else if (o instanceof Byte) {
                generator.writeNumberField("min", (Byte) o);
            } else if (o instanceof Short) {
                generator.writeNumberField("min", (Short) o);
            } else if (o instanceof Decimal) {
                generator.writeBinaryField("min", ((Decimal) o).toUnscaledBytes());
            }
        }
        if (max != null) {
            Object o = max;
            if (o instanceof String) {
                generator.writeStringField("max", (String) o);
            } else if (o instanceof Long) {
                generator.writeNumberField("max", (Long) o);
            } else if (o instanceof Integer) {
                generator.writeNumberField("max", (Integer) o);
            } else if (o instanceof Double) {
                generator.writeNumberField("max", (Double) o);
            } else if (o instanceof Float) {
                generator.writeNumberField("max", (Float) o);
            } else if (o instanceof Boolean) {
                generator.writeBooleanField("max", (Boolean) o);
            } else if (o instanceof Byte) {
                generator.writeNumberField("max", (Byte) o);
            } else if (o instanceof Short) {
                generator.writeNumberField("max", (Short) o);
            } else if (o instanceof Decimal) {
                generator.writeBinaryField("max", ((Decimal) o).toUnscaledBytes());
            }
        }
        if (nullCount != null) {
            generator.writeNumberField("nullCount", nullCount);
        }
        if (avgLen != null) {
            generator.writeNumberField("avgLen", avgLen);
        }
        if (maxLen != null) {
            generator.writeNumberField("maxLen", maxLen);
        }
        generator.writeEndObject();
    }

    public static DataFieldStats deserializeJson(JsonNode jsonNode, DataType type) {
        Object min = null;
        Object max = null;
        if (jsonNode.get("min") != null && jsonNode.get("max") != null) {
            if (type.getTypeRoot().equals(DataTypeRoot.BIGINT)) {
                min = jsonNode.get("min").asLong();
                max = jsonNode.get("max").asLong();
            } else if (type.getTypeRoot().equals(DataTypeRoot.INTEGER)
                    || type.getTypeRoot().equals(DataTypeRoot.DATE)) {
                min = jsonNode.get("min").asInt();
                max = jsonNode.get("max").asInt();
            } else if (type.getTypeRoot().equals(DataTypeRoot.DECIMAL)) {
                try {
                    DecimalType d = (DecimalType) type;
                    min =
                            Decimal.fromUnscaledBytes(
                                    jsonNode.get("min").binaryValue(),
                                    d.getPrecision(),
                                    d.getScale());
                    max =
                            Decimal.fromUnscaledBytes(
                                    jsonNode.get("max").binaryValue(),
                                    d.getPrecision(),
                                    d.getScale());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return new DataFieldStats(
                jsonNode.get("distinctCount") != null
                        ? jsonNode.get("distinctCount").asLong()
                        : null,
                min,
                max,
                jsonNode.get("nullCount") != null ? jsonNode.get("nullCount").asLong() : null,
                jsonNode.get("avgLen") != null ? jsonNode.get("avgLen").asLong() : null,
                jsonNode.get("maxLen") != null ? jsonNode.get("maxLen").asLong() : null);
    }

    public DataFieldStats copy() {
        return new DataFieldStats(distinctCount, min, max, nullCount, avgLen, maxLen);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        DataFieldStats that = (DataFieldStats) object;
        return Objects.equals(distinctCount, that.distinctCount)
                && Objects.equals(min, that.min)
                && Objects.equals(max, that.max)
                && Objects.equals(nullCount, that.nullCount)
                && Objects.equals(avgLen, that.avgLen)
                && Objects.equals(maxLen, that.maxLen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(distinctCount, min, max, nullCount, avgLen, maxLen);
    }

    @Override
    public String toString() {
        return "DataFieldStats{"
                + "distinctCount="
                + distinctCount
                + ", min="
                + min
                + ", max="
                + max
                + ", nullCount="
                + nullCount
                + ", avgLen="
                + avgLen
                + ", maxLen="
                + maxLen
                + '}';
    }
}
