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

import org.apache.flink.table.procedures.Procedure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Utility methods for {@link Procedure}. */
public class ProcedureUtil {

    private ProcedureUtil() {}

    private static final List<String> SYSTEM_PROCEDURES = new ArrayList<>();

    static {
        SYSTEM_PROCEDURES.add(CompactProcedure.NAME);
        SYSTEM_PROCEDURES.add(CompactDatabaseProcedure.NAME);
        SYSTEM_PROCEDURES.add(CreateTagProcedure.NAME);
        SYSTEM_PROCEDURES.add(DeleteTagProcedure.NAME);
        SYSTEM_PROCEDURES.add(DropPartitionProcedure.NAME);
        SYSTEM_PROCEDURES.add(MergeIntoProcedure.NAME);
        SYSTEM_PROCEDURES.add(ResetConsumerProcedure.NAME);
        SYSTEM_PROCEDURES.add(RollbackToProcedure.NAME);
    }

    public static List<String> listProcedures() {
        return Collections.unmodifiableList(SYSTEM_PROCEDURES);
    }

    public static Optional<Procedure> getProcedure(Catalog catalog, String procedureName) {
        switch (procedureName) {
            case CompactProcedure.NAME:
                return Optional.of(new CompactProcedure(catalog));
            case CompactDatabaseProcedure.NAME:
                return Optional.of(new CompactDatabaseProcedure(catalog));
            case CreateTagProcedure.NAME:
                return Optional.of(new CreateTagProcedure(catalog));
            case DeleteTagProcedure.NAME:
                return Optional.of(new DeleteTagProcedure(catalog));
            case DropPartitionProcedure.NAME:
                return Optional.of(new DropPartitionProcedure(catalog));
            case MergeIntoProcedure.NAME:
                return Optional.of(new MergeIntoProcedure(catalog));
            case ResetConsumerProcedure.NAME:
                return Optional.of(new ResetConsumerProcedure(catalog));
            case RollbackToProcedure.NAME:
                return Optional.of(new RollbackToProcedure(catalog));
            default:
                return Optional.empty();
        }
    }
}
