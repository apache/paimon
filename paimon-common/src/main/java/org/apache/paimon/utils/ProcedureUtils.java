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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ExpireConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

/** Utils for procedure. */
public class ProcedureUtils {

    public static Map<String, String> fillInPartitionOptions(
            String expireStrategy,
            String timestampFormatter,
            String timestampPattern,
            String expirationTime,
            Integer maxExpires,
            String options) {

        HashMap<String, String> dynamicOptions = new HashMap<>();
        if (!StringUtils.isNullOrWhitespaceOnly(options)) {
            dynamicOptions.putAll(ParameterUtils.parseCommaSeparatedKeyValues(options));
        }
        setTableOptions(
                dynamicOptions, CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(), expireStrategy);
        setTableOptions(
                dynamicOptions,
                CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(),
                timestampFormatter);
        setTableOptions(
                dynamicOptions, CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), timestampPattern);
        setTableOptions(
                dynamicOptions, CoreOptions.PARTITION_EXPIRATION_TIME.key(), expirationTime);
        setTableOptions(dynamicOptions, CoreOptions.PARTITION_EXPIRATION_MAX_NUM.key(), maxExpires);
        setTableOptions(dynamicOptions, CoreOptions.PARTITION_EXPIRATION_CHECK_INTERVAL.key(), "0");

        return dynamicOptions;
    }

    private static void setTableOptions(
            HashMap<String, String> dynamicOptions, String key, String value) {
        if (!StringUtils.isNullOrWhitespaceOnly(value)) {
            dynamicOptions.put(key, value);
        }
    }

    private static void setTableOptions(
            HashMap<String, String> dynamicOptions, String key, Integer value) {
        if (value != null) {
            dynamicOptions.put(key, String.valueOf(value));
        }
    }

    public static ExpireConfig.Builder fillInSnapshotOptions(
            CoreOptions tableOptions,
            Integer retainMax,
            Integer retainMin,
            String olderThanStr,
            Integer maxDeletes) {

        ExpireConfig.Builder builder = ExpireConfig.builder();
        builder.snapshotRetainMax(
                Optional.ofNullable(retainMax).orElse(tableOptions.snapshotNumRetainMax()));
        builder.snapshotRetainMin(
                Optional.ofNullable(retainMin).orElse(tableOptions.snapshotNumRetainMin()));
        builder.snapshotTimeRetain(tableOptions.snapshotTimeRetain());
        if (!StringUtils.isNullOrWhitespaceOnly(olderThanStr)) {
            long olderThanMills =
                    DateTimeUtils.parseTimestampData(olderThanStr, 3, TimeZone.getDefault())
                            .getMillisecond();
            builder.snapshotTimeRetain(
                    Duration.ofMillis(System.currentTimeMillis() - olderThanMills));
        }
        builder.snapshotMaxDeletes(
                Optional.ofNullable(maxDeletes).orElse(tableOptions.snapshotExpireLimit()));
        return builder;
    }
}
