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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.procedures.Procedure;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Utility methods for {@link Procedure}. */
public class ProcedureUtil {

    private ProcedureUtil() {}

    public static List<String> listProcedures() {
        return Collections.unmodifiableList(
                FactoryUtil.discoverIdentifiers(
                        ProcedureBase.class.getClassLoader(), ProcedureBase.class));
    }

    public static Optional<Procedure> getProcedure(Catalog catalog, ObjectPath procedurePath) {
        if (!Catalog.SYSTEM_DATABASE_NAME.equals(procedurePath.getDatabaseName())) {
            return Optional.empty();
        }
        try {
            ProcedureBase procedure =
                    FactoryUtil.discoverFactory(
                                    ProcedureBase.class.getClassLoader(),
                                    ProcedureBase.class,
                                    procedurePath.getObjectName())
                            .withCatalog(catalog);
            return Optional.of(procedure);
        } catch (FactoryException e) {
            return Optional.empty();
        }
    }
}
