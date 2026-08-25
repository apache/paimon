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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.management.DataPolicy;

import org.apache.spark.sql.connector.catalog.TableCatalog;

/** Creates a table row-filter or column-mask policy. */
public class CreatePolicyProcedure extends CreateOrReplacePolicyBaseProcedure {

    private CreatePolicyProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    protected void writePolicy(DataPolicy policy) {
        policyManagement().createPolicy(policy);
    }

    public static ProcedureBuilder builder() {
        return new Builder<CreatePolicyProcedure>() {
            @Override
            protected CreatePolicyProcedure doBuild() {
                return new CreatePolicyProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "CreatePolicyProcedure";
    }
}
