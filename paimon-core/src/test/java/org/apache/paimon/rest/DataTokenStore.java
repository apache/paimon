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

package org.apache.paimon.rest;

import java.util.HashMap;
import java.util.Map;

/** DataTokenStore is used to store data token. */
public class DataTokenStore {

    // as warehouse means one catalog instance, so we use warehouse as key to store data token
    private static final Map<String, Map<String, RESTToken>> warehouse2DataTokenStore =
            new HashMap<>();

    public static void putDataToken(String warehouse, String tableFullName, RESTToken dataToken) {
        Map<String, RESTToken> dataTokenStore = warehouse2DataTokenStore.get(warehouse);
        if (dataTokenStore == null) {
            dataTokenStore = new HashMap<>();
            warehouse2DataTokenStore.put(warehouse, dataTokenStore);
        }
        dataTokenStore.put(tableFullName, dataToken);
    }

    public static RESTToken getDataToken(String warehouse, String tableFullName) {
        Map<String, RESTToken> dataTokenStore = warehouse2DataTokenStore.get(warehouse);
        if (dataTokenStore == null) {
            return null;
        }
        return dataTokenStore.get(tableFullName);
    }

    public static void removeDataToken(String warehouse, String tableFullName) {
        Map<String, RESTToken> dataTokenStore = warehouse2DataTokenStore.get(warehouse);
        if (dataTokenStore != null && dataTokenStore.containsKey(tableFullName)) {
            dataTokenStore.remove(tableFullName);
            warehouse2DataTokenStore.put(warehouse, dataTokenStore);
        }
    }
}
