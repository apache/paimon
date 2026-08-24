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
import org.apache.paimon.management.Permission;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Lists explicitly granted permissions. */
public class ListPermissionsProcedure extends BasePermissionProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("resource_type", StringType),
                ProcedureParameter.optional("database", StringType),
                ProcedureParameter.optional("table", StringType),
                ProcedureParameter.optional("function", StringType),
                ProcedureParameter.optional("view", StringType),
                ProcedureParameter.optional("principal", StringType),
                ProcedureParameter.optional("max_results", IntegerType),
                ProcedureParameter.optional("page_token", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        field("resource_type", StringType, false),
                        field("catalog", StringType, true),
                        field("database", StringType, true),
                        field("table", StringType, true),
                        field("function", StringType, true),
                        field("view", StringType, true),
                        field("columns_json", StringType, true),
                        field("row_filter_json", StringType, true),
                        field("column_masking_json", StringType, true),
                        field("access", StringType, false),
                        field("principal", StringType, false),
                        field("expire_time", StringType, true),
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
        Integer maxResults = args.isNullAt(6) ? null : args.getInt(6);
        checkArgument(maxResults == null || maxResults > 0, "max_results must be greater than 0.");

        ListPermissionsRequest request =
                new ListPermissionsRequest(
                        resourceType,
                        args.isNullAt(1) ? null : emptyToNull(args.getString(1)),
                        args.isNullAt(2) ? null : emptyToNull(args.getString(2)),
                        args.isNullAt(3) ? null : emptyToNull(args.getString(3)),
                        args.isNullAt(4) ? null : emptyToNull(args.getString(4)),
                        args.isNullAt(5) ? null : emptyToNull(args.getString(5)),
                        args.isNullAt(7) ? null : emptyToNull(args.getString(7)),
                        maxResults);
        PagedList<Permission> page = permissionManagement().listPermissions(request);
        List<Permission> permissions = page.getElements();
        if (permissions == null || permissions.isEmpty()) {
            return new InternalRow[0];
        }

        InternalRow[] rows = new InternalRow[permissions.size()];
        for (int i = 0; i < permissions.size(); i++) {
            Permission permission = permissions.get(i);
            rows[i] =
                    newInternalRow(
                            string(permission.getResourceType().name()),
                            string(permission.getCatalog()),
                            string(permission.getDatabase()),
                            string(permission.getTable()),
                            string(permission.getFunction()),
                            string(permission.getView()),
                            json(permission.getColumns()),
                            json(permission.getRowFilter()),
                            json(permission.getColumnMasking()),
                            string(permission.getAccess()),
                            string(permission.getPrincipal()),
                            string(permission.getExpireTime()),
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
