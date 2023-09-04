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

package org.apache.paimon.tag.period;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * The DailyTagPeriodHandler class is a concrete implementation of the BaseTagPeriodHandler. It
 * specializes in handling time tags and delays based on a daily period duration.
 *
 * <p>This implementation uses a day-based DateTimeFormatter and provides additional logic to
 * convert tags into LocalDateTime instances starting at the beginning of the day.
 */
public class DailyTagPeriodHandler extends BaseTagPeriodHandler {

    public DailyTagPeriodHandler() {
        super(Duration.ofDays(1));
    }

    @Override
    protected DateTimeFormatter formatter() {
        return DAY_FORMATTER;
    }

    @Override
    public LocalDateTime tagToTime(String tag) {
        return LocalDate.parse(tag, formatter()).atStartOfDay();
    }
}
