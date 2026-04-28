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

import static dev.vortex.api.proto.TemporalMetadatas.TIME_UNIT_MICROS;
import static dev.vortex.api.proto.TemporalMetadatas.TIME_UNIT_NANOS;

import com.google.protobuf.ByteString;
import dev.vortex.proto.DTypeProtos;
import java.util.Optional;

/** Factory class for creating Vortex data type definitions. */
public final class DTypes {
    private DTypes() {}

    static DTypeProtos.DType nullType() {
        return DTypeProtos.DType.newBuilder()
                .setNull(DTypeProtos.Null.newBuilder().build())
                .build();
    }

    static DTypeProtos.DType bool(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setBool(DTypeProtos.Bool.newBuilder().setNullable(nullable).build())
                .build();
    }

    static DTypeProtos.DType int8(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setPrimitive(DTypeProtos.Primitive.newBuilder()
                        .setType(DTypeProtos.PType.I8)
                        .setNullable(nullable)
                        .build())
                .build();
    }

    static DTypeProtos.DType int16(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setPrimitive(DTypeProtos.Primitive.newBuilder()
                        .setType(DTypeProtos.PType.I16)
                        .setNullable(nullable)
                        .build())
                .build();
    }

    static DTypeProtos.DType int32(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setPrimitive(DTypeProtos.Primitive.newBuilder()
                        .setType(DTypeProtos.PType.I32)
                        .setNullable(nullable)
                        .build())
                .build();
    }

    static DTypeProtos.DType int64(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setPrimitive(DTypeProtos.Primitive.newBuilder()
                        .setType(DTypeProtos.PType.I64)
                        .setNullable(nullable)
                        .build())
                .build();
    }

    static DTypeProtos.DType float32(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setPrimitive(DTypeProtos.Primitive.newBuilder()
                        .setType(DTypeProtos.PType.F32)
                        .setNullable(nullable)
                        .build())
                .build();
    }

    static DTypeProtos.DType float64(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setPrimitive(DTypeProtos.Primitive.newBuilder()
                        .setType(DTypeProtos.PType.F64)
                        .setNullable(nullable)
                        .build())
                .build();
    }

    static DTypeProtos.DType decimal(boolean nullable, int precision, int scale) {
        return DTypeProtos.DType.newBuilder()
                .setDecimal(DTypeProtos.Decimal.newBuilder()
                        .setNullable(nullable)
                        .setPrecision(precision)
                        .setScale(scale)
                        .build())
                .build();
    }

    static DTypeProtos.DType string(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setUtf8(DTypeProtos.Utf8.newBuilder().setNullable(nullable).build())
                .build();
    }

    static DTypeProtos.DType binary(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setBinary(DTypeProtos.Binary.newBuilder().setNullable(nullable).build())
                .build();
    }

    static DTypeProtos.DType dateDays(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setExtension(DTypeProtos.Extension.newBuilder()
                        .setId("vortex.date")
                        .setStorageDtype(DTypes.int32(nullable))
                        .setMetadata(ByteString.copyFrom(TemporalMetadatas.DATE_DAYS.get()))
                        .build())
                .build();
    }

    static DTypeProtos.DType dateMillis(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setExtension(DTypeProtos.Extension.newBuilder()
                        .setId("vortex.date")
                        .setStorageDtype(DTypes.int64(nullable))
                        .setMetadata(ByteString.copyFrom(TemporalMetadatas.DATE_MILLIS.get()))
                        .build())
                .build();
    }

    static DTypeProtos.DType timeSeconds(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setExtension(DTypeProtos.Extension.newBuilder()
                        .setId("vortex.time")
                        .setStorageDtype(DTypes.int32(nullable))
                        .setMetadata(ByteString.copyFrom(TemporalMetadatas.TIME_SECONDS.get()))
                        .build())
                .build();
    }

    static DTypeProtos.DType timeMillis(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setExtension(DTypeProtos.Extension.newBuilder()
                        .setId("vortex.time")
                        .setStorageDtype(DTypes.int32(nullable))
                        .setMetadata(ByteString.copyFrom(TemporalMetadatas.TIME_MILLIS.get()))
                        .build())
                .build();
    }

    static DTypeProtos.DType timeMicros(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setExtension(DTypeProtos.Extension.newBuilder()
                        .setId("vortex.time")
                        .setStorageDtype(DTypes.int64(nullable))
                        .setMetadata(ByteString.copyFrom(TemporalMetadatas.TIME_MICROS.get()))
                        .build())
                .build();
    }

    static DTypeProtos.DType timeNanos(boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setExtension(DTypeProtos.Extension.newBuilder()
                        .setId("vortex.time")
                        .setStorageDtype(DTypes.int64(nullable))
                        .setMetadata(ByteString.copyFrom(TemporalMetadatas.TIME_NANOS.get()))
                        .build())
                .build();
    }

    static DTypeProtos.DType timestampMillis(Optional<String> timeZone, boolean nullable) {
        return timestamp(TemporalMetadatas.TIME_UNIT_MILLIS, timeZone, nullable);
    }

    static DTypeProtos.DType timestampMicros(Optional<String> timeZone, boolean nullable) {
        return timestamp(TIME_UNIT_MICROS, timeZone, nullable);
    }

    static DTypeProtos.DType timestampNanos(Optional<String> timeZone, boolean nullable) {
        return timestamp(TIME_UNIT_NANOS, timeZone, nullable);
    }

    private static DTypeProtos.DType timestamp(byte timeUnit, Optional<String> timeZone, boolean nullable) {
        return DTypeProtos.DType.newBuilder()
                .setExtension(DTypeProtos.Extension.newBuilder()
                        .setId("vortex.timestamp")
                        .setStorageDtype(DTypes.int64(nullable))
                        .setMetadata(ByteString.copyFrom(TemporalMetadatas.timestamp(timeUnit, timeZone)))
                        .build())
                .build();
    }
}
