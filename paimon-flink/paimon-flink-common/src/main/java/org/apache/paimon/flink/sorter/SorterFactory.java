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

package org.apache.paimon.flink.sorter;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Sorter factory for zorder and order by and so on... */
public class SorterFactory {

    private static final Pattern orderByPattern =
            Pattern.compile("([a-z]+)\\((([^,]*)(,[^,]*)*)\\)");

    public static TableSorter getSorter(
            StreamTableEnvironment batchTEnv, Table origin, String orderByClause) {
        Matcher matcher = orderByPattern.matcher(orderByClause);

        if (!matcher.find()) {
            throw new RuntimeException("cannot resolve order by clause: " + orderByClause);
        }

        String orderType = matcher.group(1);
        List<String> columns =
                Arrays.asList(matcher.group(2).split(",")).stream()
                        .map(s -> s.trim())
                        .collect(Collectors.toList());

        switch (OrderType.of(orderType)) {
            case ORDER:
                return new OrderSorter(batchTEnv, origin, columns);
            case ZORDER:
                return new ZorderSorter(batchTEnv, origin, columns);
            default:
                throw new IllegalArgumentException("cannot match order type: " + orderType);
        }
    }

    enum OrderType {
        ORDER("order"),
        ZORDER("zorder");

        private String orderType;

        OrderType(String orderType) {
            this.orderType = orderType;
        }

        @Override
        public String toString() {
            return "order type: " + orderType;
        }

        public static OrderType of(String orderType) {
            if (ORDER.orderType.equals(orderType)) {
                return ORDER;
            } else if (ZORDER.orderType.equals(orderType)) {
                return ZORDER;
            }

            throw new IllegalArgumentException("cannot match type: " + orderType + " for ordering");
        }
    }
}
