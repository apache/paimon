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

import org.apache.paimon.PagedList;
import org.apache.paimon.management.ListPermissionsRequest;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionScope;
import org.apache.paimon.management.PrincipalType;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Lists direct and optionally resource-inherited permissions on an exact target. */
public class ListPermissionsProcedure extends BasePermissionProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("resource_type", StringType),
                ProcedureParameter.optional("scope", StringType),
                ProcedureParameter.optional("database", StringType),
                ProcedureParameter.optional("table", StringType),
                ProcedureParameter.optional("function", StringType),
                ProcedureParameter.optional("view", StringType),
                ProcedureParameter.optional("principal_type", StringType),
                ProcedureParameter.optional("principal", StringType),
                ProcedureParameter.optional("access", StringType),
                ProcedureParameter.optional("include_inherited", BooleanType),
                ProcedureParameter.optional("max_results", IntegerType),
                ProcedureParameter.optional("page_token", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        field("resource_type", StringType, false),
                        field("scope", StringType, false),
                        field("database", StringType, true),
                        field("table", StringType, true),
                        field("function", StringType, true),
                        field("view", StringType, true),
                        field("access", StringType, false),
                        field("principal_type", StringType, false),
                        field("principal", StringType, false),
                        field("expire_time", StringType, true),
                        field("inherited_from_json", StringType, true),
                        field("next_page_token", StringType, true)
                    });

    private ListPermissionsProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        ResourceType resourceType =
                enumValue(args.getString(0), ResourceType.class, PARAMETERS[0].name());
        Integer maxResults = args.isNullAt(10) ? null : args.getInt(10);
        ListPermissionsRequest request =
                new ListPermissionsRequest(
                        resourceType,
                        optionalEnum(
                                args.isNullAt(1) ? null : args.getString(1),
                                PermissionScope.class,
                                PARAMETERS[1].name()),
                        args.isNullAt(2) ? null : emptyToNull(args.getString(2)),
                        args.isNullAt(3) ? null : emptyToNull(args.getString(3)),
                        args.isNullAt(4) ? null : emptyToNull(args.getString(4)),
                        args.isNullAt(5) ? null : emptyToNull(args.getString(5)),
                        optionalEnum(
                                args.isNullAt(6) ? null : args.getString(6),
                                PrincipalType.class,
                                PARAMETERS[6].name()),
                        args.isNullAt(7) ? null : emptyToNull(args.getString(7)),
                        args.isNullAt(8) ? null : emptyToNull(args.getString(8)),
                        !args.isNullAt(9) && args.getBoolean(9),
                        args.isNullAt(11) ? null : emptyToNull(args.getString(11)),
                        maxResults);
        PagedList<PermissionAssignment> page = permissionManagement().listPermissions(request);
        List<PermissionAssignment> assignments = page.getElements();
        if (assignments == null || assignments.isEmpty()) {
            return new InternalRow[0];
        }

        InternalRow[] rows = new InternalRow[assignments.size()];
        for (int i = 0; i < assignments.size(); i++) {
            PermissionAssignment assignment = assignments.get(i);
            rows[i] =
                    newInternalRow(
                            string(assignment.getResource().getType().name()),
                            string(assignment.getScope().name()),
                            string(assignment.getResource().getDatabase()),
                            string(assignment.getResource().getTable()),
                            string(assignment.getResource().getFunction()),
                            string(assignment.getResource().getView()),
                            string(assignment.getAccess()),
                            string(assignment.getPrincipal().getType().name()),
                            string(assignment.getPrincipal().getId()),
                            string(assignment.getExpireTime()),
                            json(assignment.getInheritedFrom()),
                            string(page.getNextPageToken()));
        }
        return rows;
    }

    private static StructField field(
            String name, org.apache.spark.sql.types.DataType type, boolean nullable) {
        return new StructField(name, type, nullable, Metadata.empty());
    }

    private static UTF8String string(String value) {
        return value == null ? null : UTF8String.fromString(value);
    }

    private static UTF8String json(Object value) {
        return value == null ? null : string(JsonSerdeUtil.toFlatJson(value));
    }

    public static ProcedureBuilder builder() {
        return new Builder<ListPermissionsProcedure>() {
            @Override
            protected ListPermissionsProcedure doBuild() {
                return new ListPermissionsProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ListPermissionsProcedure";
    }
}
