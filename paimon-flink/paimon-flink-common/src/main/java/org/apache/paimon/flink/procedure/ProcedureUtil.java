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

package org.apache.paimon.flink.procedure;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.procedures.Procedure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Utility methods for {@link Procedure}. */
public class ProcedureUtil {

    private ProcedureUtil() {}

    private static final String SYSTEM_DATABASE_NAME = "system";
    private static final List<String> SYSTEM_PROCEDURES = new ArrayList<>();
    private static final Map<ObjectPath, Procedure> SYSTEM_PROCEDURES_MAP = new HashMap<>();

    public static List<String> listProcedures(String dbName) {
        checkDatabase(dbName);
        return Collections.unmodifiableList(SYSTEM_PROCEDURES);
    }

    public static Optional<Procedure> getProcedure(ObjectPath procedurePath) {
        checkDatabase(procedurePath.getDatabaseName());
        return Optional.ofNullable(SYSTEM_PROCEDURES_MAP.get(procedurePath));
    }

    private static void checkDatabase(String dbName) {
        if (!dbName.equals(SYSTEM_DATABASE_NAME)) {
            throw new CatalogException(
                    "Currently, Paimon catalog only supports built-in procedures in database '"
                            + SYSTEM_DATABASE_NAME
                            + "'.");
        }
    }
}
