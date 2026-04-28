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

package dev.vortex.api.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import dev.vortex.proto.ScalarProtos;
import java.math.BigDecimal;
import java.util.Optional;

/** Factory class for creating Vortex scalar values with their associated data types. */
public final class Scalars {
    private Scalars() {}

    public static ScalarProtos.Scalar nullNull() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.nullType())
                .build();
    }

    public static ScalarProtos.Scalar bool(boolean value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setBoolValue(value)
                        .build())
                .setDtype(DTypes.bool(false))
                .build();
    }

    public static ScalarProtos.Scalar nullBool() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.bool(true))
                .build();
    }

    public static ScalarProtos.Scalar int8(byte value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.int8(false))
                .build();
    }

    public static ScalarProtos.Scalar nullInt8() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.int8(true))
                .build();
    }

    public static ScalarProtos.Scalar int16(short value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.int16(false))
                .build();
    }

    public static ScalarProtos.Scalar nullInt16() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.int16(true))
                .build();
    }

    public static ScalarProtos.Scalar int32(int value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.int32(false))
                .build();
    }

    public static ScalarProtos.Scalar nullInt32() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.int32(true))
                .build();
    }

    public static ScalarProtos.Scalar int64(long value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.int64(false))
                .build();
    }

    public static ScalarProtos.Scalar nullInt64() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.int64(true))
                .build();
    }

    public static ScalarProtos.Scalar float32(float value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(
                        ScalarProtos.ScalarValue.newBuilder().setF32Value(value).build())
                .setDtype(DTypes.float32(false))
                .build();
    }

    public static ScalarProtos.Scalar nullFloat32() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.float32(true))
                .build();
    }

    public static ScalarProtos.Scalar float64(double value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(
                        ScalarProtos.ScalarValue.newBuilder().setF64Value(value).build())
                .setDtype(DTypes.float64(false))
                .build();
    }

    public static ScalarProtos.Scalar nullFloat64() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.float64(true))
                .build();
    }

    public static ScalarProtos.Scalar string(String value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setStringValue(value)
                        .build())
                .setDtype(DTypes.string(false))
                .build();
    }

    public static ScalarProtos.Scalar nullString() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.string(true))
                .build();
    }

    public static ScalarProtos.Scalar decimal(BigDecimal decimal, int precision, int scale) {
        byte[] littleEndian = EndianUtils.littleEndianDecimal(decimal);
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setBytesValue(ByteString.copyFrom(littleEndian))
                        .build())
                .setDtype(DTypes.decimal(false, precision, scale))
                .build();
    }

    public static ScalarProtos.Scalar nullDecimal(int precision, int scale) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder().setNullValue(NullValue.NULL_VALUE))
                .setDtype(DTypes.decimal(true, precision, scale))
                .build();
    }

    public static ScalarProtos.Scalar bytes(byte[] value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setBytesValue(ByteString.copyFrom(value))
                        .build())
                .setDtype(DTypes.binary(false))
                .build();
    }

    public static ScalarProtos.Scalar nullBytes() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.binary(true))
                .build();
    }

    public static ScalarProtos.Scalar dateDays(int value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.dateDays(false))
                .build();
    }

    public static ScalarProtos.Scalar nullDateDays() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.dateDays(true))
                .build();
    }

    public static ScalarProtos.Scalar dateMillis(long value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.dateMillis(false))
                .build();
    }

    public static ScalarProtos.Scalar nullDateMillis() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.dateMillis(true))
                .build();
    }

    public static ScalarProtos.Scalar timeSeconds(int value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.timeSeconds(false))
                .build();
    }

    public static ScalarProtos.Scalar nullTimeSeconds() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.timeSeconds(true))
                .build();
    }

    public static ScalarProtos.Scalar timeMillis(int value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.timeMillis(false))
                .build();
    }

    public static ScalarProtos.Scalar nullTimeMillis() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.timeMillis(true))
                .build();
    }

    public static ScalarProtos.Scalar timeMicros(long value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.timeMicros(false))
                .build();
    }

    public static ScalarProtos.Scalar nullTimeMicros() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.timeMicros(true))
                .build();
    }

    public static ScalarProtos.Scalar timeNanos(long value) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.timeNanos(false))
                .build();
    }

    public static ScalarProtos.Scalar nullTimeNanos() {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.timeNanos(true))
                .build();
    }

    public static ScalarProtos.Scalar timestampMillis(long value, Optional<String> timeZone) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.timestampMillis(timeZone, false))
                .build();
    }

    public static ScalarProtos.Scalar nullTimestampMillis(Optional<String> timeZone) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.timestampMillis(timeZone, true))
                .build();
    }

    public static ScalarProtos.Scalar timestampMicros(long value, Optional<String> timeZone) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.timestampMicros(timeZone, false))
                .build();
    }

    public static ScalarProtos.Scalar nullTimestampMicros(Optional<String> timeZone) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.timestampMicros(timeZone, true))
                .build();
    }

    public static ScalarProtos.Scalar timestampNanos(long value, Optional<String> timeZone) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setInt64Value(value)
                        .build())
                .setDtype(DTypes.timestampNanos(timeZone, false))
                .build();
    }

    public static ScalarProtos.Scalar nullTimestampNanos(Optional<String> timeZone) {
        return ScalarProtos.Scalar.newBuilder()
                .setValue(ScalarProtos.ScalarValue.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build())
                .setDtype(DTypes.timestampNanos(timeZone, true))
                .build();
    }
}
