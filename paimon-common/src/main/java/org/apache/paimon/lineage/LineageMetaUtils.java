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

package org.apache.paimon.lineage;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Utils class for lineage meta. */
public class LineageMetaUtils {
    private static final int MAX_VARCHAR_LENGTH = 10240;

    public static final String SOURCE_TABLE_LINEAGE = "source_table_lineage";

    public static final String SINK_TABLE_LINEAGE = "sink_table_lineage";

    public static RowType tableLineageRowType() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "database_name", new VarCharType(MAX_VARCHAR_LENGTH)));
        fields.add(new DataField(1, "table_name", new VarCharType(MAX_VARCHAR_LENGTH)));
        fields.add(new DataField(2, "job_name", new VarCharType(MAX_VARCHAR_LENGTH)));
        fields.add(new DataField(3, "create_time", new TimestampType()));
        return new RowType(fields);
    }

    public static List<String> tableLineagePrimaryKeys() {
        return Arrays.asList("database_name", "table_name", "job_name");
    }
}
