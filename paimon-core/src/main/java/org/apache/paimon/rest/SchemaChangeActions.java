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

public class SchemaChangeActions {
    public static final String FIELD_ACTION = "action";
    public static final String SET_OPTION_ACTION = "setOption";
    public static final String REMOVE_OPTION_ACTION = "removeOption";
    public static final String UPDATE_COMMENT_ACTION = "updateComment";
    public static final String ADD_COLUMN_ACTION = "addColumn";
    public static final String RENAME_COLUMN_ACTION = "renameColumn";
    public static final String DROP_COLUMN_ACTION = "dropColumn";
    public static final String UPDATE_COLUMN_TYPE_ACTION = "updateColumnType";
    public static final String UPDATE_COLUMN_NULLABILITY_ACTION = "updateColumnNullability";
    public static final String UPDATE_COLUMN_COMMENT_ACTION = "updateColumnComment";
    public static final String UPDATE_COLUMN_POSITION_ACTION = "updateColumnPosition";
}
