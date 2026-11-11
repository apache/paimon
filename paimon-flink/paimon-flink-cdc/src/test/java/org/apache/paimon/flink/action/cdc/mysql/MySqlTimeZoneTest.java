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

package org.apache.paimon.flink.action.cdc.mysql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks the MySQL test time zone across daylight-saving transitions and JVM time zones. */
class MySqlTimeZoneTest {

    @ParameterizedTest
    @CsvSource({
        "2026-03-08T06:30:00Z,GMT+07:00",
        "2026-03-08T07:30:00Z,GMT+07:00",
        "2026-11-01T04:30:00Z,GMT+07:00",
        "2026-11-01T06:30:00Z,America/New_York",
        "2026-11-01T07:30:00Z,America/Los_Angeles",
        "2026-07-01T12:00:00Z,UTC",
        "2026-01-01T12:00:00Z,Asia/Shanghai"
    })
    void testValidatorOffsetMatchesServer(String timestamp, String jvmZone) throws Exception {
        Properties config = new Properties();
        try (InputStream input = getClass().getResourceAsStream("/mysql/my.cnf")) {
            config.load(input);
        }
        assertThat(config.getProperty("default-time-zone"))
                .isEqualTo(MySqlActionITCaseBase.MYSQL_SERVER_TIME_ZONE);
        ZoneId serverZone = ZoneId.of(config.getProperty("default-time-zone"));
        Instant instant = Instant.parse(timestamp);
        LocalDateTime localTime = LocalDateTime.now(Clock.fixed(instant, ZoneId.of(jvmZone)));
        // MySqlValidator uses the JVM's local date-time to look up the server zone offset.
        assertThat(serverZone.getRules().getOffset(localTime))
                .isEqualTo(serverZone.getRules().getOffset(instant));
    }

    @Test
    void testTimestampFixtureKeepsThreeHourDifference() {
        LocalDateTime inserted = LocalDateTime.parse("2023-03-23T15:00:10.123456");
        LocalDateTime read =
                inserted.atZone(ZoneId.of(MySqlActionITCaseBase.MYSQL_SYSTEM_TIME_ZONE))
                        .withZoneSameInstant(
                                ZoneId.of(MySqlActionITCaseBase.MYSQL_SERVER_TIME_ZONE))
                        .toLocalDateTime();
        assertThat(read).isEqualTo(LocalDateTime.parse("2023-03-23T18:00:10.123456"));
    }
}
