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

package org.apache.paimon.flink.pulsar;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities for Pulsar options parsing.
 */
public class PulsarOptionsUtils {

    public static int getSendTimeoutMs(Map<String, String> parameters) {
        String interval = parameters.getOrDefault(PulsarLogOptions.SEND_TIMEOUT_MS.key(), "30000");
        return Integer.parseInt(interval);
    }

    public static long getTransactionTimeout(Map<String, String> parameters) {
        String value =
                parameters.getOrDefault(PulsarLogOptions.TRANSACTION_TIMEOUT.key(), "3600000");
        return Long.parseLong(value);
    }

    public static long getMaxBlockTimeMs(Map<String, String> parameters) {
        String value = parameters.getOrDefault(PulsarLogOptions.MAX_BLOCK_TIME_MS.key(), "100000");
        return Long.parseLong(value);
    }

    public static Map<String, String> toCaceInsensitiveParams(Map<String, String> parameters) {
        return parameters.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                t -> t.getKey().toLowerCase(Locale.ROOT), t -> t.getValue()));
    }

    public static Map<String, Object> getProducerParams(Map<String, String> parameters) {
        return parameters.keySet().stream()
                .filter(k -> k.startsWith(PulsarLogOptions.PRODUCER_CONFIG_PREFIX))
                .collect(
                        Collectors.toMap(
                                k -> k.substring(PulsarLogOptions.PRODUCER_CONFIG_PREFIX.length()),
                                k -> parameters.get(k)));
    }
}
