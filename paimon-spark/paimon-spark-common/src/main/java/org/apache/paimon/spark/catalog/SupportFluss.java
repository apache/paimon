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

package org.apache.paimon.spark.catalog;

import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.connector.catalog.Table;

/** Catalog support for Fluss LakeStream tables backed by Paimon. */
public interface SupportFluss {

    String LAKESTREAM_ENABLED = "lakestream.enabled";

    default boolean isFlussTable(Table table) {
        if (!(table instanceof SparkTable)) {
            return false;
        }
        return isFlussTable(((SparkTable) table).getTable());
    }

    default boolean isFlussTable(org.apache.paimon.table.Table table) {
        return table instanceof FileStoreTable
                && Boolean.parseBoolean(table.options().get(LAKESTREAM_ENABLED));
    }
}
