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

package org.apache.paimon.fs;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Credential suppliers that a plugin {@link FileIO} looks up by id, so it can resolve fresh
 * credentials on every request instead of the ones it was created with.
 */
public final class CredentialsSupplierRegistry {

    /** Option that passes the id of a registered supplier to the {@link FileIO} being created. */
    public static final String SUPPLIER_ID = "fs.credentials-supplier.id";

    private static final Map<String, Supplier<Map<String, String>>> SUPPLIERS =
            new ConcurrentHashMap<>();

    private CredentialsSupplierRegistry() {}

    /** Registers a supplier of credential options and returns its id. */
    public static String register(Supplier<Map<String, String>> supplier) {
        String id = UUID.randomUUID().toString();
        SUPPLIERS.put(id, supplier);
        return id;
    }

    @Nullable
    public static Supplier<Map<String, String>> get(String id) {
        return SUPPLIERS.get(id);
    }

    public static void unregister(String id) {
        SUPPLIERS.remove(id);
    }
}
