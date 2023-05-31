/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.avro;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.LocalZonedTimestampType;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** Test for row data to avro converters. */
public class RowDataToAvroConvertersTest {

    @Test
    public void testTimestampWithTimeType() {
        LocalZonedTimestampType zonedTimestampType = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3);
        RowDataToAvroConverters.RowDataToAvroConverter rowDataToAvroConverter =
                RowDataToAvroConverters.createConverter(zonedTimestampType);

        OffsetDateTime offsetDateTime =
                OffsetDateTime.of(LocalDate.of(2023, 1, 1), LocalTime.of(0, 0, 0), ZoneOffset.UTC);

        Schema schema = AvroSchemaConverter.convertToSchema(zonedTimestampType);
        long timestamp = offsetDateTime.toInstant().toEpochMilli();
        Timestamp millis = Timestamp.fromEpochMillis(timestamp);
        Object converted = rowDataToAvroConverter.convert(schema, millis);

        AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter =
                AvroToRowDataConverters.createConverter(zonedTimestampType);

        Object original = avroToRowDataConverter.convert(converted);
        Assertions.assertEquals(millis, original);
    }
}
