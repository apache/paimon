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

package org.apache.paimon.utils;

import org.apache.paimon.options.Options;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/** Helper for propagating timestamp-timezone-conversion options through the option chain. */
public final class TimezoneOptionUtils {

    public static final String CONVERSION_ENABLED = "timestamp.timezone.conversion.enabled";
    public static final String HIVE_WRITER_TZ = "hive.writer.timezone";
    public static final String SYSTEM_TZ = "system.timezone";

    /** Unmodifiable list of all option keys handled here. */
    public static final List<String> KEYS =
            Collections.unmodifiableList(
                    Arrays.asList(CONVERSION_ENABLED, HIVE_WRITER_TZ, SYSTEM_TZ));

    private TimezoneOptionUtils() {}

    /** Enable UTC→local timestamp conversion (Hive writes UTC in Parquet). */
    public static void enableUtcToLocal(Options opts, TimeZone systemTz) {
        opts.set(HIVE_WRITER_TZ, "UTC");
        opts.set(SYSTEM_TZ, systemTz.getID());
        opts.set(CONVERSION_ENABLED, "true");
    }

    public static void enableUtcToLocal(Map<String, String> target, TimeZone systemTz) {
        target.put(HIVE_WRITER_TZ, "UTC");
        target.put(SYSTEM_TZ, systemTz.getID());
        target.put(CONVERSION_ENABLED, "true");
    }

    public static boolean isEnabled(Options opts) {
        return opts.getBoolean(CONVERSION_ENABLED, false);
    }

    public static void enableWriterToLocal(Map<String, String> target, TimeZone writerTz, TimeZone systemTz) {
        target.put(HIVE_WRITER_TZ, writerTz.getID());
        target.put(SYSTEM_TZ, systemTz.getID());
        target.put(CONVERSION_ENABLED, "true");
    }
}
